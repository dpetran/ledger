(ns fluree.db.ledger.vocabs.www.w3.org.ns.dcat
  (:require [fluree.db.ledger.vocabs.core :as vocabs]
            [fluree.db.util.log :as log]
            [clojure.string :as str]))

(def merge-in
  {"title"       {:type         ["http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"]
                  :fluree/type  :string
                  :rdfs/label   {:en "Title of Resource or Catalog"}
                  :id           "title"
                  :rdfs/comment "Title of Resource or Catalog"
                  :iri          "http://www.w3.org/ns/dcat#title"}
   "description" {:type         ["http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"]
                  :fluree/type  :string
                  :rdfs/label   {:en "Description of Resource or Catalog"}
                  :id           "description"
                  :rdfs/comment "Description of Resource or Catalog"
                  :iri          "http://www.w3.org/ns/dcat#description"}})



(defn process
  "Reads original json-ld vocabulary, goes through our basic x-form
  process to optimize it for @context reads, and proceeds to 'enrich'
  it with functions in this namespace specific to schema.org vocab,
  lastly writes the output so it can be easily picked up by a transactor
  when processing a transaction."
  []
  (let [ontology    "http://www.w3.org/ns/dcat#"
        output-file "resources/contexts/www.w3.org/ns/dcat.edn"]
    (-> (vocabs/load-ontology ontology)
        (vocabs/onotology->context ontology {:ignore ["http://www.w3.org/2004/02/skos"]
                                             :merge  merge-in})

        (vocabs/save-context output-file))))


(comment

  (process)

  (keys (process))

  (get (process) "distribution")

  )