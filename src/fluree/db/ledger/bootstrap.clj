(ns fluree.db.ledger.bootstrap
  (:require [clojure.string :as str]
            [fluree.db.ledger.bootstrap.genesis :as genesis]
            [fluree.db.flake :as flake]
            [fluree.crypto :as crypto]
            [fluree.db.storage.core :as storage]
            [fluree.db.util.json :as json]
            [fluree.db.constants :as const]
            [fluree.db.session :as session]
            [fluree.db.ledger.indexing :as indexing]
            [fluree.db.ledger.txgroup.txgroup-proto :as txproto]
            [fluree.db.util.async :refer [go-try <?]]
            [fluree.db.util.log :as log])
  (:import (fluree.db.flake Flake)))

(def initial-block-num 1)
(def initial-t -1)
(def initial-block-t -2)

(defn get-block-hash
  "Note this must be in the proper sort order before executing"
  [flakes]
  (->> flakes
       (mapv #(vector (.-s %) (.-p %) (.-o %) (.-t %) (.-op %) (.-m %)))
       (json/stringify)
       (crypto/sha3-256)))


(defn master-auth-flake
  [t pred->id ident->id auth-subid master-authority]
  (let [db-setting-id (flake/->sid const/$_setting 0)
        true-fn-sid   (flake/->sid const/$_fn 0)
        false-fn-sid  (flake/->sid const/$_fn 1)
        rule-sid      (flake/->sid const/$_rule 0)
        role-sid      (flake/->sid const/$_role 0)]
    (when-not master-authority
      (throw (ex-info (str "No Master Authority provided when bootstrapping.")
                      {:status 500 :error :db/unexpected-error})))
    (when-not (get pred->id "_auth/id")
      (throw (ex-info (str "Unable to determine _auth/id predicate id when bootstrapping.")
                      {:status 500 :error :db/unexpected-error})))
    ;(when-not (get pred->id "_setting/id")
    ;  (throw (ex-info (str "Unable to determine _setting/id predicate id when bootstrapping.")
    ;                  {:status 500 :error :db/unexpected-error})))
    ;(when-not (get pred->id "_setting/defaultAuth")
    ;  (throw (ex-info (str "Unable to determine _setting/defaultAuth predicate id when bootstrapping.")
    ;                  {:status 500 :error :db/unexpected-error})))
    [
     ;; add a true predicate function
     (flake/new-flake true-fn-sid (get pred->id "_fn/name") "true" t true)
     (flake/new-flake true-fn-sid (get pred->id "_fn/doc") "Allows access to any rule or spec this is attached to." t true)
     (flake/new-flake true-fn-sid (get pred->id "_fn/code") "true" t true)
     ;; add a false predicate function (just for completeness)
     (flake/new-flake false-fn-sid (get pred->id "_fn/name") "false" t true)
     (flake/new-flake false-fn-sid (get pred->id "_fn/doc") "Denies access to any rule or spec this is attached to." t true)
     (flake/new-flake false-fn-sid (get pred->id "_fn/code") "false" t true)

     ;; add a 'root' rule
     (flake/new-flake rule-sid (get pred->id "_rule/id") "root" t true)
     (flake/new-flake rule-sid (get pred->id "_rule/doc") "Root rule, gives full access" t true)
     (flake/new-flake rule-sid (get pred->id "_rule/collection") "*" t true)
     (flake/new-flake rule-sid (get pred->id "_rule/predicates") "*" t true)
     (flake/new-flake rule-sid (get pred->id "_rule/fns") true-fn-sid t true)
     (flake/new-flake rule-sid (get pred->id "_rule/ops") (get ident->id ["_tag/id" "_rule/ops:all"]) t true)

     ;; add a 'root' role
     (flake/new-flake role-sid (get pred->id "_role/id") "root" t true)
     (flake/new-flake role-sid (get pred->id "_role/doc") "Root role." t true)
     (flake/new-flake role-sid (get pred->id "_role/rules") rule-sid t true)

     ;; add auth record, and assign root rule
     (flake/new-flake auth-subid (get pred->id "_auth/id") master-authority t true)
     (flake/new-flake auth-subid (get pred->id "_auth/roles") role-sid t true)

     ;; add ledger that uses master auth
     (flake/new-flake db-setting-id (get pred->id "_setting/ledgers") auth-subid t true)
     (flake/new-flake db-setting-id (get pred->id "_setting/language") (get ident->id ["_tag/id" "_setting/language:en"]) t true)
     (flake/new-flake db-setting-id (get pred->id "_setting/id") "root" t true)]))

(defn parse-db-name
  [db-name]
  (if (sequential? db-name)
    db-name
    (str/split db-name #"/")))

(defn new-ledger
  "Initializes a blank ledger with network `network` and id `dbid`, validating
  that neither a ledger nor a first block on disk exists with the same network
  and id"
  [{:keys [group] :as conn} network dbid]
  (go-try
   (if-not (or (txproto/ledger-exists? group network dbid)
               (<? (storage/block conn network dbid 1)))
     (session/blank-db conn [network dbid])
     (throw (ex-info (str "Ledger " network "/$" dbid " already exists!"
                          " Create unsuccessful.")
                     {:status 500, :error :db/unexpected-error})))))

(defn initial-block
  [cmd sig txid ts]
  (let [{:keys [fparts pred->id ident->id]}
        genesis/flake-parts

        master-authid    (crypto/account-id-from-message cmd sig)
        auth-subid       (flake/->sid 6 0)
        authority-flakes (master-auth-flake initial-t pred->id ident->id auth-subid master-authid)

        meta-flakes      [(flake/new-flake initial-t (get pred->id "_tx/id") txid initial-t true)
                          (flake/new-flake initial-t (get pred->id "_tx/nonce") ts initial-t true)
                          (flake/new-flake initial-block-t (get pred->id "_block/number") 1 initial-block-t true)
                          (flake/new-flake initial-block-t (get pred->id "_block/instant") ts initial-block-t true)
                          (flake/new-flake initial-block-t (get pred->id "_block/transactions") -1 initial-block-t true)
                          (flake/new-flake initial-block-t (get pred->id "_block/transactions") -2 initial-block-t true)]

        first-flakes     (->> fparts
                              (reduce (fn [acc [s p o]]
                                        (->> (flake/new-flake s p o initial-t true)
                                             (conj acc)))
                                      (flake/sorted-set-by flake/cmp-flakes-spot-novelty)))

        hashable-flakes  (->> meta-flakes
                              (into authority-flakes)
                              (into first-flakes))

        hash            (get-block-hash hashable-flakes)
        block-flakes    [(flake/new-flake initial-block-t (get pred->id "_block/hash") hash initial-block-t true)
                         (flake/new-flake initial-block-t (get pred->id "_block/ledgers") auth-subid initial-block-t true)]
        flakes          (into hashable-flakes block-flakes)]
    {:block  initial-block-num
     :t      initial-block-t
     :flakes flakes
     :hash   hash
     :txns   {txid {:t   initial-t
                    :cmd cmd
                    :sig sig}}}))

(defn initialize-db
  [ledger {:keys [block flakes t] :as initial-block}]
  (let [flake-size  (flake/size-bytes flakes)
        flake-count (count flakes)

        {:keys [index-pred ref-pred]}
        genesis/flake-parts

        post-flakes (filter (fn [^Flake f]
                              (-> f .-p index-pred))
                            flakes)
        opst-flakes (filter (fn [^Flake f]
                              (-> f .-p ref-pred))
                            flakes)]
    (-> ledger
        (assoc :block  block
               :t      t
               :ecount genesis/ecount)
        (update :stats assoc :flakes flake-count, :size flake-size)
        (update-in [:novelty :spot] into flakes)
        (update-in [:novelty :psot] into flakes)
        (update-in [:novelty :post] into post-flakes)
        (update-in [:novelty :opst] into opst-flakes)
        (update-in [:novelty :tspo] into flakes)
        (assoc-in [:novelty :size] flake-size))))


(defn bootstrap-db
  "Bootstraps a new db from a signed new-db message."
  [{:keys [conn group]} {:keys [cmd sig]}]
  (go-try
   (let [timestamp      (System/currentTimeMillis)
         txid           (crypto/sha3-256 cmd)
         [network dbid] (-> cmd json/parse :db parse-db-name)

         first-block    (initial-block cmd sig txid timestamp)

         {:keys [block fork stats] :as new-ledger}
         (-> conn
             (new-ledger network dbid)
             <?
             (initialize-db first-block)
             indexing/index
             <?)]

     (<? (storage/write-block conn network dbid first-block))

     ;; TODO should create a new command to register new DB that first checks
     ;;      raft
     (<? (txproto/register-genesis-block-async group network dbid))

     ;; write out new index point
     (<? (txproto/initialized-ledger-async group txid network dbid block fork (:indexed stats)))

     new-ledger)))


(defn create-network-bootstrap-command
  "For a new network, we create a new signed command to create master network db."
  [db-name private-key]
  (let [auth  (crypto/account-id-from-private private-key)
        epoch (System/currentTimeMillis)
        cmd   (-> {:type   :new-db
                   :db     db-name
                   :auth   auth
                   :doc    "Master network database."
                   :nonce  epoch
                   :expire (+ 300000 epoch)}
                  (json/stringify))
        sig   (crypto/sign-message cmd private-key)]
    {:cmd cmd
     :sig sig}))
