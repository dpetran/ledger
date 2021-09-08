(ns fluree.db.ledger.jsonld.test-base
  (:require [clojure.test :refer :all]
            [fluree.db.ledger.test-helpers :as test]
            [fluree.db.ledger.docs.getting-started.basic-schema :as basic]
            [fluree.db.api :as fdb]
            [clojure.core.async :as async]
            [fluree.db.util.log :as log]))

(use-fixtures :once test/test-system)

(deftest transact-schema-org
  (testing "Transact nested schema.org data into a new ledger")
  (let [txn          [{"@context"                  "https://schema.org",
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
                                                              "name"  "Douglas Adams"}}}]
        schema-resp  @(fdb/transact (basic/get-conn) test/ledger-json-ld txn)
        tempids-keys (->> schema-resp :tempids keys (into #{}))]

    ;; status should be 200
    (is (= 200 (:status schema-resp)))

    ;; block should be 2
    (is (= 2 (:block schema-resp)))

    ;; tempid keys should be the IRIs used above
    (is (= #{"https://www.wikidata.org/wiki/Q836821"
             "https://www.wikidata.org/wiki/Q3107329"
             "https://www.wikidata.org/wiki/Q42"}
           tempids-keys))))


(deftest basic-schema-org-query
  (testing "Select * from IRI")
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
    (is (= 7 (count (keys query-data))))))


(deftest schema-org-classes-query
  (testing "Class & Hierarchy query support")
  (let [db                 (basic/get-db test/ledger-json-ld {:syncTo 2})
        movie-q            {:select {"?s" ["*"]}
                            :where  [["?s" "rdf:type" "https://schema.org/Movie"]]}
        movie-a-alias-q    {:select {"?s" ["*"]}            ;;  use 'a' instead of 'rdf:type', should be interchangeable
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
    (is (= 3 (count thing-resp)))))


(deftest schema-org-context-query
  (testing "Using a prefix in a query, you can shorten the IRIs")
  (let [db          (basic/get-db test/ledger-json-ld {:syncTo 2})
        movie-q     {:context {"myprefix" "https://schema.org/"} ;; prefix mapping
                     :select  {"?s" ["*"]}
                     :where   [["?s" "rdf:type" "myprefix:Movie"]]}
        movie-q2    {:context "https://schema.org/"         ;; just a URL
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
    (is (= 1 (count movie-resp3)))))


(deftest update-with-iri
  (testing "Update data on a subject using it's @id iri")
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
    (is (= 42 (get query-resp "commentCount")))))


(deftest transaction-fn
  (testing "Transaction function works as expected.")
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
    (is (= 52 (get query-resp "commentCount")))))


(deftest json-ld-tests
  (transact-schema-org)
  (basic-schema-org-query)
  (schema-org-classes-query)
  (schema-org-context-query)
  (update-with-iri)
  (transaction-fn))