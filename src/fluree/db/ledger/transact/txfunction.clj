(ns fluree.db.ledger.transact.txfunction
  (:require [fluree.db.dbfunctions.core :as dbfunctions]
            [fluree.db.util.async :refer [<?? channel?]]))

;; functions related to transaction functions

;; TODO - can probably parse function string to final 'lisp form' when generating TxFunction
(defrecord TxFunction [fn-str f])

(defn to-tx-function
  "Returns a TxFunction record containing the original function string and a compile function ready to execute."
  [fn-str {:keys [db-root] :as tx-state}]
  (let [fn-str  (subs fn-str 1)                                ;; remove preceding '#'
        f       (<?? (dbfunctions/parse-fn db-root fn-str "txn" nil))]
    (->TxFunction fn-str f)))

(defn tx-fn?
  "Returns true if a transaction function"
  [x]
  (instance? TxFunction x))

;; TODO - below should probably be using a permissioned db, not db-root
(defn execute
  "Returns a core async channel with response"
  [{:keys [f]} _id pred-info {:keys [auth db-root instant fuel]}]
  (let [ctx     {:db      db-root
                 :instant instant
                 :sid     _id
                 :pid     (pred-info :id)
                 :auth_id auth
                 :state   fuel}
        res     (f ctx)]
    ;; TODO - I believe all functions are now go-chans, as their args may be async functions so all are wrapped
    (if (channel? res)
      (<?? res) res)))


