(ns fluree.db.ledger.transact.json-ld
  (:require [fluree.db.util.async :refer [<? <?? go-try merge-into? channel?]]
            [fluree.db.util.log :as log]
            [fluree.db.dbproto :as dbproto]
            [fluree.db.constants :as const]
            [fluree.db.api :as fdb]
            [fluree.db.ledger.transact.identity :as identity]
            [fluree.db.ledger.transact.tempid :as tempid]
            [fluree.db.util.iri :as iri-util]
            [fluree.db.ledger.transact.schema :as tx-schema]))


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


(defn normalize-txi
  "Expands transaction item from compacted iris to full iri."
  [txi context]
  (reduce-kv (fn [acc k v]
               (assoc acc (iri-util/expand k context) (iri-util/expand v context)))
             {} txi))


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
    (merge default-context
           (iri-util/expanded-context txi-context default-context))
    default-context))


(defn- base-statement
  "Return the portion of a 'statement' for the subject, which can be used for individual
  predicate+objects to add to."
  [{:keys [collector] :as tx-state} local-context txi idx]
  (let [iri          (get-in txi ["@id" :val])
        expanded-iri (when iri
                       (iri-util/expand iri local-context)) ;; first expand iri with local context
        types        (when-let [item-type (get-in txi ["@type" :val])]
                       (<?? (tx-schema/resolve-types item-type local-context idx tx-state)))
        collection   (collector expanded-iri types)
        _action      (get-in txi ["@action" :val])
        _meta        (get-in txi ["@meta" :val])
        action       (resolve-action _action)
        id           (if expanded-iri
                       (<?? (identity/resolve-iri expanded-iri collection nil idx tx-state))
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
               (if-let [key-ctx (get local-context k)]
                 (assoc acc (:id key-ctx) (assoc key-ctx :val v :as k))
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
                         ctx*       (->> (get normalized "@type")
                                         (#(if (sequential? %) % [%]))
                                         (map #(get-in ctx [(:val %) :context]))
                                         (apply merge ctx))]
                     (generate-statement tx-state obj idx* ctx*)))
        p        (pred-info :id)
        o        (if children
                   (:id (first children))                   ;; children may be multiple further nested, but first one is one we want
                   obj)
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


(defn predicate-statements
  [p-o-pairs base-smt idx {:keys [db-before] :as tx-state}]
  (reduce (fn [acc [pred obj]]
            (let [idx*       (conj idx (:as obj))
                  pred-info  (or (predicate-details pred (:collection base-smt) db-before)
                                 (tx-schema/generate-property pred obj idx* tx-state))
                  multi?     (pred-info :multi)
                  statements (if multi?
                               (->> (if (sequential? (:val obj)) (into #{} (:val obj)) [(:val obj)])
                                    (map-indexed (partial statement-obj base-smt pred-info tx-state idx*))
                                    (apply concat))
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
            p-o-pairs (into (predicate-statements p-o-pairs base-smt idx tx-state))
            blank? (conj base-smt)
            delete-subject? (conj (assoc base-smt :action :retract-subject)))))


(defn tx?
  "Returns true if the transaction supplied looks like JSON-LD."
  [tx]
  (boolean
    (or
      (get tx "@graph")
      (get-in tx [0 "@id"])
      (get-in tx [0 "@context"]))))


(defn get-tx-context
  "Returns the context to be used for the transaction.
  If there is a @context defined for the tx, merges it into the db's context,
  else returns the db's context."
  [db tx]
  (if-let [tx-ctx (get tx "@context")]
    (let [db-ctx (-> db :schema :prefix)]
      (merge db-ctx (iri-util/expanded-context tx-ctx db-ctx)))
    (-> db :schema :prefix)))


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
                                          :error :db/invalid-transaction}))
        (empty? txi) (throw (ex-info (str "Empty or nil transaction item found in transaction.")
                                     {:status 400
                                      :error :db/invalid-transaction}))
        :else (->> (generate-statement tx-state txi [i] nil)
                   (reduce conj! acc)
                   (recur r (inc i)))))))




;; TODO
;; - add prefix-resolver to tx-state


(comment
  (def db (<?? (fdb/db (:conn user/system) "prefix/d")))

  (def sys-coll (system-collections db))
  sys-coll


  (def prefix-resolver (iri-util/compact-fn sys-context))
  (prefix-resolver "http://www.w3.org/2000/01/rdf-schema#subClass")

  (def context (iri-util/expanded-context {"nc"        "http://release.niem.gov/niem/niem-core/4.0/#",
                                           "j"         "http://release.niem.gov/niem/domains/jxdm/6.0/#",
                                           "age"       "nc:PersonAgeMeasure",
                                           "value"     "nc:MeasureIntegerValue",
                                           "units"     "nc:TimeUnitCode",
                                           "hairColor" "j:PersonHairColorCode",
                                           "name"      "nc:PersonName",
                                           "given"     "nc:PersonGivenName",
                                           "surname"   "nc:PersonSurName",
                                           "suffix"    "nc:PersonNameSuffixText",
                                           "nickname"  "nc:PersonPreferredName",}
                                          #_sys-context))

  context
  (def ctx-compactor (iri-util/compact-fn context))

  (ctx-compactor "http://release.niem.gov/niem/niem-core/4.0/#2PersonName")

  )
