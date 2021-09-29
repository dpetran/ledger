(ns fluree.db.ledger.transact.json-ld
  (:require [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.util.log :as log]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]
            [fluree.db.ledger.transact.identity :as identity]
            [fluree.db.ledger.transact.tempid :as tempid]
            [fluree.db.ledger.transact.schema :as tx-schema]
            [fluree.db.dbfunctions.core :as dbfunctions]
            [fluree.db.ledger.transact.txfunction :as txfunction]
            [fluree.json-ld :as json-ld]
            [fluree.json-ld.util :refer [sequential]]))


(defn system-collections
  "Returns map of defined base-iris -> collection."
  [db]
  (->> db
       :schema
       :coll
       (filter #(number? (key %)))
       vals
       (filter :base-iri)
       (reduce #(assoc %1 (:base-iri %2) (:name %2)) {})))


(defn collector-fn
  "Function that given an iri, return the collection that is supposed to be used."
  [sys-collections]
  (let [match-iris (->> sys-collections
                        keys
                        (sort-by #(* -1 (count %))))        ;; want longest iris checked first
        match-fns  (mapv
                     (fn [base-iri]
                       (let [re         (re-pattern (str "^" base-iri))
                             collection (get sys-collections base-iri)]
                         (fn [iri]
                           (when (re-find re iri)
                             collection))))
                     match-iris)
        default    (get sys-collections "")]
    (fn [iri types]
      (cond
        (some #(= const/$rdfs:Class %) types) "_predicate"

        (and (nil? iri) default)
        default

        :else
        (or (some (fn [match-fn] (match-fn iri)) match-fns)
            (throw (ex-info (str "The iri does not match any collections, and no default collection is specified: "
                                 iri
                                 " Either specify _collection/baseIRI for a collection that will match, or set a "
                                 "collection as a default by setting _collection/baseIRI to '' (empty string).")
                            {:status 400
                             :error  :db/invalid-transaction})))))))


(defn build-collector-fn
  [db]
  (let [sys-collections (system-collections db)]
    (collector-fn sys-collections)))


(defn compact-txi
  "With a system context, compacts txi iris to use prefixes defined inside
  of the _prefix collection."
  [txi prefix-resolver]
  (reduce-kv (fn [acc k v]
               (assoc acc (or (prefix-resolver k) k)
                          (or (prefix-resolver v) v)))
             {} txi))


(defn system-predicate?
  "Returns true if a predicate is a special/reserved item.
  This includes anything starting with '@' (JSON-LD), or starting with '_' (Fluree)."
  [s]
  (case (first s)
    \@ true
    \_ true
    false))


;; TODO - resolve-action is duplicated with json namespace - look to consolidate somewhere.
(defn resolve-action
  "Returns one of 3 possible action types based one _action and if the
  k-v pairs of the JSON transaction are empty:
  - :add - adding transaction items (which can be over-ridden by a nil value of a key pair)
  - :retract - retracting transaction items (all k-v pairs will attempt deletes)
  - :retract-subject - retract all values for a given subject."
  [_action]
  (if (= :delete (keyword _action))
    :retract
    :add))


(defn predicate-details
  "Returns function for predicate to retrieve any predicate details"
  [predicate collection db]
  (when-let [pred-id (or
                       (get-in db [:schema :pred (str collection "/" predicate) :id])
                       (get-in db [:schema :pred predicate :id]))]
    (fn [property] (dbproto/-p-prop db property pred-id))))


(defn- local-context
  "If an @context exists for a specific transaction item, resolve it then
  merges into the default context"
  [txi default-context]
  (if-let [txi-context (get txi "@context")]
    (json-ld/parse-context default-context txi-context)
    default-context))


(defn- base-statement
  "Return the portion of a 'statement' for the subject, which can be used for individual
  predicate+objects to add to."
  [{:keys [collector] :as tx-state} local-context txi idx]
  (let [iri        (-> (get-in txi ["@id" :val])
                       (json-ld/expand local-context))
        types      (when-let [item-type (get-in txi ["@type" :val])]
                     (<?? (tx-schema/resolve-types item-type local-context idx tx-state)))
        collection (collector iri types)
        _action    (get-in txi ["@action" :val])
        _meta      (get-in txi ["@meta" :val])
        action     (resolve-action _action)
        id         (if iri
                     (<?? (identity/resolve-iri iri collection idx tx-state))
                     (tempid/construct nil idx tx-state collection))]
    {:iri        iri
     :id         id
     :tempid?    (tempid/TempId? id)
     :action     action
     :collection collection
     :o-tempid?  nil
     :types      types
     :context    local-context}))


(defn normalize-txi
  "Takes raw txi, and puts it into final form.

  Values will be maps where the actual value is stored as the key :val.
  Value maps include the context (which may have type information, if we need to create
  new predicates/properties."
  [txi local-context]
  (reduce-kv (fn [acc k v]
               (if-let [[iri details] (json-ld/details k local-context)]
                 (assoc acc iri (assoc details :val v :as k))
                 (assoc acc k {:val v :as k})))
             {} txi))


(defn- has-child?
  "Returns true the provided object value contains a nested transaction item."
  [pred-info object]
  (and (= :ref (pred-info :type)) (map? object)))

(declare generate-statement)

(defn- statement-obj
  [base-smt pred-info tx-state idx i obj]
  (let [idx*     (if i (conj idx i) idx)
        children (when (has-child? pred-info obj)
                   (let [ctx        (local-context obj (:context base-smt))
                         normalized (normalize-txi obj ctx)
                         ctx*       (->> (:type normalized)
                                         (map #(get-in ctx [(:val %) :context]))
                                         (apply merge ctx))]
                     (generate-statement tx-state obj idx* ctx*)))
        p        (pred-info :id)
        o        (cond
                   children
                   (:id (first children))                   ;; children may be multiple further nested, but first one is one we want

                   (dbfunctions/tx-fn? obj)
                   (-> (txfunction/to-tx-function obj tx-state)
                       (txfunction/execute (:id base-smt) pred-info tx-state))

                   :else obj)
        smt      (assoc base-smt :pred-info pred-info
                                 :p p
                                 :o o
                                 :idx idx)]
    (if children
      (into [smt] children)
      [smt])))


(defn type-statements
  "Creates type statements. Types will already have been resolved to a subject-id
  by this point, so no need to lookup ids, just place in"
  [base-smt idx {:keys [db-before] :as tx-state}]
  (let [rdf-type-pid const/$rdf:type
        pred-info    (fn [property] (dbproto/-p-prop db-before property rdf-type-pid))]
    (reduce
      (fn [acc type]
        (conj acc (assoc base-smt :pred-info pred-info
                                  :p rdf-type-pid
                                  :o type
                                  :idx idx)))
      [] (:types base-smt))))


(defn build-reverse-statement
  "If a reverse reference is used from the context (i.e., {@reverse 'some-iri'})
   we will create a statement for the parent. We only allow this if the parent
   already exists in the DB. If it doesn't exist, the transaction could simply
   transact the parent instead, and either use refs or a child transaction from
   the parent.

   The value of the IRI could either be:
   - a full string IRI; e.g., https://www.wikidata.org/wiki/Q836821
   - a compact IRI string using a prefix; e.g., wiki:Q836821
   - a map with a single @id key using either of the above options; e.g.,
     {'@id' 'https://www.wikidata.org/wiki/Q836821'} or
     {'@id' 'wiki:Q836821'}"
  [pred-info {:keys [id action]} {:keys [val]} idx context {:keys [db-before] :as tx-state}]
  (go-try
    (let [multi-val?  (sequential? val)
          invalid-ref (fn [idx* msg] (throw
                                       (ex-info (str msg ". Error found at index: "
                                                     (if multi-val? idx* idx) ".")
                                                {:status 400
                                                 :error  :db/invalid-reverse-ref})))

          map->id     (fn [m idx*] (if-let [iri (get m "@id")]
                                     (if (= (list "@id") (keys m))
                                       iri
                                       (invalid-ref idx* (str "A map used as a reverse reference can only contain "
                                                              "a single @id key. Provided: " m)))
                                     (invalid-ref idx* (str "If a map is provided as a reverse ref value, must contain @id. Provided: " m))))
          base        {:tempid?    false
                       :action     action
                       :collection nil                      ;; not needed here
                       :types      nil                      ;; not needed here
                       :context    context
                       :pred-info  pred-info
                       :p          (pred-info :id)
                       :o          id
                       :o-tempid?  (tempid/TempId? id)}]
      (loop [[val & r] (sequential val)
             i    0
             smts []]
        (if val
          (let [idx*   (conj idx i)
                parent (cond
                         (map? val)
                         (-> val (map->id idx*) (json-ld/expand context))

                         (string? val)
                         (json-ld/expand val context)

                         :else (invalid-ref idx* (str "Unable to retrieve IRI or identity from reverse ref: " val)))
                smt    (assoc base
                         :iri parent
                         :id (or (<? (dbproto/-subid db-before parent))
                                 (invalid-ref idx* (str "Reverse refs in transactions must "
                                                        "already exist in DB. Provided: " parent)))
                         :idx (if multi-val? idx idx*))]
            (recur r (inc i) (conj smts smt)))
          smts)))))


(defn predicate-statements
  [p-o-pairs base-smt idx context {:keys [db-before] :as tx-state}]
  (reduce (fn [acc [pred obj]]
            (let [idx*       (conj idx (:as obj))
                  pred-info  (or (predicate-details pred (:collection base-smt) db-before)
                                 (tx-schema/generate-property pred obj idx* tx-state))
                  multi?     (pred-info :multi)
                  reverse?   (contains? obj :reverse)
                  statements (cond
                               reverse?
                               (<?? (build-reverse-statement pred-info base-smt obj idx* context tx-state))

                               multi?
                               (->> (if (sequential? (:val obj)) (into #{} (:val obj)) [(:val obj)])
                                    (map-indexed (partial statement-obj base-smt pred-info tx-state idx*))
                                    (apply concat))

                               :else
                               (statement-obj base-smt pred-info tx-state idx* nil (:val obj)))]
              (into acc statements)))
          [] p-o-pairs))


(defn generate-statement
  "parent-ctx will be supplied if there are nested children -- it will already
   have the tx-context merged in from the parent."
  [{:keys [tx-context] :as tx-state} txi idx parent-ctx]
  (let [ctx             (local-context txi (or parent-ctx tx-context))
        txi*            (normalize-txi txi ctx)             ;; normalize the txi with the provided context
        p-o-pairs       (not-empty (filter #(not (system-predicate? (key %))) txi*))
        base-smt        (base-statement tx-state ctx txi* nil)
        blank?          (and (nil? p-o-pairs)
                             (nil? (:types base-smt)))
        delete-subject? (and blank? (= :retract (:action base-smt)))]
    (cond-> []
            (:types base-smt) (into (type-statements base-smt idx tx-state))
            p-o-pairs (into (predicate-statements p-o-pairs base-smt idx ctx tx-state))
            blank? (conj base-smt)
            delete-subject? (conj (assoc base-smt :action :retract-subject)))))


(defn tx?
  "Returns true if the transaction supplied looks like JSON-LD."
  [tx]
  (boolean
    (or
      (get tx "@graph")
      (get-in tx [0 "@context"])
      (get-in tx [0 "@id"]))))


(defn get-tx-context
  "Returns the context to be used for the transaction.
  If there is a @context defined for the tx, merges it into the db's context,
  else returns the db's context."
  [db tx]
  (let [db-ctx (-> db :schema :prefix)
        tx-ctx (get tx "@context")]
    (json-ld/parse-context db-ctx tx-ctx)))


(defn generate-statements
  [tx-state tx]
  ;; TODO - if we maintain tx-context here, delete from tx-state
  (let [tx-data (or (get tx "@graph")
                    (when (sequential? tx) tx)
                    (throw (ex-info (str "Invalid transaction.") {:status 400 :error :db/invalid-transaction})))]
    (loop [[txi & r] tx-data
           i   0
           acc (transient [])]
      (cond
        (nil? txi) (persistent! acc)
        (not (map? txi)) (throw (ex-info (str "All transaction items must be maps/objects, at least one is not.")
                                         {:status 400
                                          :error  :db/invalid-transaction}))
        (empty? txi) (throw (ex-info (str "Empty or nil transaction item found in transaction.")
                                     {:status 400
                                      :error  :db/invalid-transaction}))
        :else (->> (generate-statement tx-state txi [i] nil)
                   (reduce conj! acc)
                   (recur r (inc i)))))))

