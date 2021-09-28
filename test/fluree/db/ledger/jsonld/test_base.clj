(ns fluree.db.ledger.jsonld.test-base
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [fluree.db.util.log :as log]
            [fluree.db.util.schema :as schema-util])
  (:import (fluree.db.flake Flake)))

(use-fixtures :once test/test-system)

(def base-tx [{"@context"                  "https://schema.org",
               "@id"                       "https://www.wikidata.org/wiki/Q836821",
               "@type"                     "Movie",
               "name"                      "The Hitchhiker's Guide to the Galaxy",
               "disambiguatingDescription" "2005 British-American comic science fiction film directed by Garth Jennings",
               "titleEIDR"                 "10.5240/B752-5B47-DBBE-E5D4-5A3F-N",
               "isBasedOn"                 {"@id"    "https://www.wikidata.org/wiki/Q3107329",
                                            "@type"  "Book",
                                            "name"   "The Hitchhiker's Guide to the Galaxy",
                                            "isbn"   "0-330-25864-8",
                                            "author" {"@id"   "https://www.wikidata.org/wiki/Q42"
                                                      "@type" "Person"
                                                      "name"  "Douglas Adams"}}}])

(deftest transact-schema-org
  (testing "Transact nested schema.org data into a new ledger"
    (let [schema-resp  @(fdb/transact (basic/get-conn) test/ledger-json-ld base-tx)
          tempids-keys (->> schema-resp :tempids keys (into #{}))]

      ;; status should be 200
      (is (= 200 (:status schema-resp)))

      ;; block should be 2
      (is (= 2 (:block schema-resp)))

      ;; tempid keys should be the IRIs used above
      (is (= #{"https://www.wikidata.org/wiki/Q836821"
               "https://www.wikidata.org/wiki/Q3107329"
               "https://www.wikidata.org/wiki/Q42"}
             tempids-keys)))))


(deftest basic-schema-org-query
  (testing "Select * from IRI"
    (let [query      {:selectOne ["*"]
                      :from      "https://www.wikidata.org/wiki/Q836821"
                      :opts      {:meta true}}
          db         (basic/get-db test/ledger-json-ld {:syncTo 2})
          query-resp @(fdb/query db query)
          query-data (:result query-resp)]

      ;; block should be 2
      (is (= 2 (:block query-resp)))

      ;; @id should be present and equal to query
      (is (= "https://www.wikidata.org/wiki/Q836821" (get query-data "@id")))

      ;; 7 total keys - 6 properties in original transaction + :_id
      (is (= 7 (count (keys query-data)))))))


(deftest schema-org-classes-query
  (testing "Class & Hierarchy query support"
    (let [db                 (basic/get-db test/ledger-json-ld {:syncTo 2})
          movie-q            {:select {"?s" ["*"]}
                              :where  [["?s" "rdf:type" "https://schema.org/Movie"]]}
          movie-a-alias-q    {:select {"?s" ["*"]}          ;;  use 'a' instead of 'rdf:type', should be interchangeable
                              :where  [["?s" "a" "https://schema.org/Movie"]]}
          book-q             {:select {"?s" ["*"]}
                              :where  [["?s" "rdf:type" "https://schema.org/Book"]]}
          person-q           {:select {"?s" ["*"]}
                              :where  [["?s" "rdf:type" "https://schema.org/Person"]]}
          creative-q         {:select {"?s" ["*"]}
                              :where  [["?s" "rdf:type" "https://schema.org/CreativeWork"]]}
          thing-q            {:select {"?s" ["*"]}
                              :where  [["?s" "rdf:type" "https://schema.org/Thing"]]}
          movie-resp         @(fdb/query db movie-q)
          movie-a-alias-resp @(fdb/query db movie-a-alias-q)
          book-resp          @(fdb/query db book-q)
          person-resp        @(fdb/query db person-q)
          creative-resp      @(fdb/query db creative-q)
          thing-resp         @(fdb/query db thing-q)]

      ;; should be one Movie
      (is (= 1 (count movie-resp)))

      ;; Movie with 'rdf:type' query should be same as Movie with 'a' query
      (is (= movie-a-alias-resp movie-resp))

      ;; should be one Book
      (is (= 1 (count book-resp)))

      ;; should be one Person
      (is (= 1 (count person-resp)))

      ;; should be two CreativeWork (Movie and Book are subclasses of CreativeWork)
      (is (= 2 (count creative-resp)))

      ;; should be three Thing (Movie, Book and Person are subclasses of Thing)
      (is (= 3 (count thing-resp))))))


(deftest schema-org-context-query
  (testing "Using a prefix in a query, you can shorten the IRIs"
    (let [db          (basic/get-db test/ledger-json-ld {:syncTo 2})
          movie-q     {:context {"myprefix" "https://schema.org/"} ;; prefix mapping
                       :select  {"?s" ["*"]}
                       :where   [["?s" "rdf:type" "myprefix:Movie"]]}
          movie-q2    {:context "https://schema.org/"       ;; just a URL
                       :select  {"?s" ["*"]}
                       :where   [["?s" "rdf:type" "Movie"]]}
          prop-q      {:context {"myprefix" "https://schema.org/"} ;; prefix for a property
                       :select  {"?s" ["*"]}
                       :where   [["?s" "myprefix:titleEIDR" "10.5240/B752-5B47-DBBE-E5D4-5A3F-N"]]}
          movie-resp  @(fdb/query db movie-q)
          movie-resp2 @(fdb/query db movie-q2)
          movie-resp3 @(fdb/query db prop-q)]

      ;; should be one Movie
      (is (= 1 (count movie-resp)))
      (is (= 1 (count movie-resp2)))
      (is (= 1 (count movie-resp3))))))


(deftest query-context-consistency
  (testing "Context used for transaction produces (almost) same results in query."
    (let [db           (basic/get-db test/ledger-json-ld {:syncTo 2})
          basic-q      {:context   "https://schema.org/"
                        :selectOne ["*"]
                        :from      "https://www.wikidata.org/wiki/Q836821"}
          anlyt-q      {:context   "https://schema.org/"
                        :selectOne {"?s" ["*"]}
                        :where     [["?s" "rdf:type" "Movie"]]}
          anlyt-q*     {:context   "https://schema.org"     ;; <--- note: no trailing slash as per above
                        :selectOne {"?s" ["*"]}
                        :where     [["?s" "rdf:type" "Movie"]]}
          basic-resp   @(fdb/query db basic-q)
          anlyt-resp   @(fdb/query db anlyt-q)
          anlyt-resp*  @(fdb/query db anlyt-q*)
          base-tx-keys (->> base-tx first keys (into #{}))
          resp-keys    (->> basic-resp keys (into #{}))]

      ;; query results with and without trailing slash in @context should be identical
      (is (= anlyt-resp anlyt-resp*))

      ;; query results should be identical
      (is (= basic-resp anlyt-resp))

      ;; keys should be identical to original transaction (minus @context and :_id)
      (is (= (disj base-tx-keys "@context") (disj resp-keys :_id))))))


(deftest query-context-specific
  (testing "Context used for transaction produces (almost) same results in query."
    (let [db         (basic/get-db test/ledger-json-ld {:syncTo 2})
          context    {"@vocab" "https://schema.org/"
                      "wiki"   "https://www.wikidata.org/wiki/"
                      "id"     "@id"}
          basic-q    {:context   context
                      :selectOne ["*"]
                      :from      "https://www.wikidata.org/wiki/Q836821"}
          anlyt-q    {:context   context
                      :selectOne {"?s" ["*"]}
                      :where     [["?s" "rdf:type" "Movie"]]}
          basic-resp @(fdb/query db basic-q)
          anlyt-resp @(fdb/query db anlyt-q)]

      ;; query results should be identical
      (is (= basic-resp anlyt-resp))

      ;; @id should now be labeled as "id"
      (is (contains? basic-resp "id"))

      ;; wiki IRI should be shortened
      (is (= "wiki:Q836821" (get basic-resp "id")))

      ;; @type which also uses schema.org should be shortened
      (is (= ["Movie"] (get basic-resp "@type")))))

  (testing "Context and explicit compact-iri predicates in :select"
    (let [db         (basic/get-db test/ledger-json-ld {:syncTo 2})
          context    {"@vocab" "https://schema.org/"
                      "title"  "https://schema.org/titleEIDR"}
          basic-q    {:context   context
                      :selectOne ["name" "title"]
                      :from      "https://www.wikidata.org/wiki/Q836821"}
          anlyt-q    {:context   context
                      :selectOne {"?s" ["name" "title"]}
                      :where     [["?s" "rdf:type" "Movie"]]}
          basic-resp @(fdb/query db basic-q)
          anlyt-resp @(fdb/query db anlyt-q)]

      (log/warn "basic-resp" basic-resp)
      (log/warn "anlyt-resp" anlyt-resp)

      ;; query results should be identical
      (is (= basic-resp anlyt-resp))

      ;; @id should now be labeled as "id"
      (is (= (get basic-resp "name")
             (get-in base-tx [0 "name"])))

      (is (= (get basic-resp "title")
             (get-in base-tx [0 "titleEIDR"]))))))


(deftest update-with-iri
  (testing "Update data on a subject using it's @id iri"
    (let [tx         [{"@context"     "https://schema.org"
                       "@id"          "https://www.wikidata.org/wiki/Q836821"
                       "commentCount" 42}]
          tx-resp    @(fdb/transact (basic/get-conn) test/ledger-json-ld tx)
          db         (basic/get-db test/ledger-json-ld {:syncTo (:block tx-resp)})
          query      {:selectOne ["*"]
                      :from      "https://www.wikidata.org/wiki/Q836821"}
          query-resp @(fdb/query db query)]

      ;; tx-result status should be 200
      (is (= 200 (:status tx-resp)))

      ;; 8 total keys - 7 properties in original transaction + :_id
      (is (= 8 (count (keys query-resp))))

      ;; commentCount should be 42 as set in tx
      (is (= 42 (get query-resp "https://schema.org/commentCount"))))))


(deftest transaction-fn
  (testing "Transaction function works as expected."
    (let [tx         [{"@context"     "https://schema.org"
                       "@id"          "https://www.wikidata.org/wiki/Q836821"
                       "commentCount" "#(+ (?pO) 10)"}]
          tx-resp    @(fdb/transact (basic/get-conn) test/ledger-json-ld tx)
          db         (basic/get-db test/ledger-json-ld {:syncTo (:block tx-resp)})
          query      {:selectOne ["*"]
                      :from      "https://www.wikidata.org/wiki/Q836821"}
          query-resp @(fdb/query db query)]

      ;; tx-result status should be 200
      (is (= 200 (:status tx-resp)))

      ;; commentCount should now be 52
      (is (= 52 (get query-resp "https://schema.org/commentCount"))))))


(deftest subject-values-return-iris
  (testing "Subjects with an @id should have that value shortened by context"
    (let [db    (basic/get-db test/ledger-json-ld {:syncTo 2})
          query {:context   {"myprefix" "https://schema.org/"
                             "wiki"     "https://www.wikidata.org/wiki/"} ;; prefix for a property
                 :selectOne "?id"
                 :where     [["?s" "myprefix:titleEIDR" "10.5240/B752-5B47-DBBE-E5D4-5A3F-N"]
                             ["?s" "@id" "?id"]]}
          resp  @(fdb/query db query)]
      ;; should be one Movie
      (is (= "wiki:Q836821" resp)))))


(deftest subid-api-call
  (testing "Subject id correctly resolved for IRIs"
    (let [db      (basic/get-db test/ledger-json-ld {:syncTo 2})
          known   @(fdb/subid db "https://www.wikidata.org/wiki/Q836821")
          unknown @(fdb/subid db "https://some.unknown/url")]
      ;; TODO - add a test for a subid that uses a default DB context

      (is (integer? known))

      (is (nil? unknown)))))


(deftest add-new-db-prefix
  (testing "You should be able to add, and then use new default prefixes."
    (let [tx         [{:_id    "_prefix"
                       :prefix "wiki"
                       :iri    "https://www.wikidata.org/wiki/"}
                      {:_id    "_prefix"
                       :prefix "schema"
                       :iri    "https://schema.org/"}]
          tx-resp    @(fdb/transact (basic/get-conn) test/ledger-json-ld tx)
          db         (basic/get-db test/ledger-json-ld {:syncTo (:block tx-resp)})
          query      {:selectOne ["*"]
                      :from      "https://www.wikidata.org/wiki/Q836821"}
          query-resp @(fdb/query db query)

          subid      @(fdb/subid db "wiki:Q836821")]

      (is (= "wiki:Q836821" (get query-resp "@id")))

      (is (= ["schema:Movie"] (get query-resp "@type")))

      (is (= "The Hitchhiker's Guide to the Galaxy" (get query-resp "schema:name")))

      ;; using default db prefix, subid lookups should work
      (is (integer? subid)))))


(deftest subsequent-tx-with-same-preds
  (testing "Subsequent transaction with same auto-generated predicates are not generated again"
    (let [tx            (assoc-in base-tx [0 "@id"] "http://some.subject/new#42")
          tx-resp       @(fdb/transact (basic/get-conn) test/ledger-json-ld tx)
          non-tx-flakes (filter #(>= (.-s ^Flake %) 0) (:flakes tx-resp)) ;; flakes not included tx-metadata
          schema-flakes (filter schema-util/is-schema-flake? (:flakes tx-resp))]

      (is (= 6 (count non-tx-flakes)))

      ;; re-using all the same schema attributes as original transaction, should be no new schema-related flakes
      (is (= 0 (count schema-flakes))))))


(deftest transaction-with-custom-context
  (testing "Using a custom context with a transaction works."
    (let [tx         [{"@context" {"comments"   "https://schema.org/commentCount"
                                   "someprefix" "https://www.wikidata.org/wiki/"}
                       "@id"      "someprefix:Q836821"
                       "comments" "#(+ (?pO) 10)"}]
          tx-resp    @(fdb/transact (basic/get-conn) test/ledger-json-ld tx)
          tempids    (->> tx-resp :tempids keys (into #{}))
          db         (basic/get-db test/ledger-json-ld {:syncTo (:block tx-resp)})
          query      {:context   "https://schema.org/"
                      :selectOne ["*"]
                      :from      "https://www.wikidata.org/wiki/Q836821"}
          query-resp @(fdb/query db query)]

      (is (empty? tempids))                                 ;; should have updated existing subject

      (is (= 62 (get query-resp "commentCount"))))))


(deftest multi-cardinality-schema-gen
  (testing "Values wrapped in a vector are assumed to be multi-cardinality."
    (let [tx         [{"@context" "https://schema.org/"
                       "@id"      "https://www.wikidata.org/wiki/Q836821"
                       "award"    ["Fluree Movie Award" "People's Choice"]}]
          tx-resp    @(fdb/transact (basic/get-conn) test/ledger-json-ld tx)
          db         (basic/get-db test/ledger-json-ld {:syncTo (:block tx-resp)})
          query      {:context   "https://schema.org/"
                      :selectOne ["*"]
                      :from      "https://www.wikidata.org/wiki/Q836821"}
          query-resp @(fdb/query db query)]

      (is (sequential? (get query-resp "award")))

      (is (= 2 (count (get query-resp "award")))))))


;; TODO - test a new predicate/class with enough info in context to generate
;; TODO - rdfs/subPropertyOf
;; TODO - language, @en, etc.
;; TODO - @reverse context

(deftest json-ld-tests
  (transact-schema-org)
  (basic-schema-org-query)
  (schema-org-classes-query)
  (schema-org-context-query)
  (query-context-consistency)
  (query-context-specific)
  (update-with-iri)
  (transaction-fn)
  (subject-values-return-iris)
  (subid-api-call)
  (add-new-db-prefix)
  (subsequent-tx-with-same-preds)
  (transaction-with-custom-context)
  (multi-cardinality-schema-gen))