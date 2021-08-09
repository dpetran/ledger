(ns fluree.db.ledger.vocabs.core
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [fluree.db.util.json :as json]
            [fluree.db.util.iri :as iri-util]
            [clojure.pprint :as pprint]
            [fluree.db.util.core :as util :refer [try* catch*]]
            [fluree.db.util.log :as log]))


(defn sequential
  "Takes input x, and if not already sequential, makes it so."
  [x]
  (when x
    (if (sequential? x)
      x
      [x])))


(defn remove-base-iri
  [full-iri base-iri]
  (if (and base-iri (str/starts-with? full-iri base-iri))
    (subs full-iri (count base-iri))
    full-iri))


(defn parse-language-items
  "Parses a language item into a map containing they
  keyword of the language as the key and the value the string."
  [language-items lang-prefs]
  (let [default-lang    (util/keyword->str (first lang-prefs))
        language-items* (cond
                          ;; just a string value, turn into language vector using default lang
                          (string? language-items)
                          [{"@language" default-lang
                            "@value"    language-items}]

                          ;; language vector, but only one item with no language specified - turn into proper format with default language
                          (and (= 1 (count language-items))
                               (nil? (get-in language-items [0 "@language"])))
                          (assoc-in language-items [0 "@language"] default-lang)

                          ;; looks like properly formatted language vector
                          :else (sequential language-items))]
    (reduce
      #(let [lang (get %2 "@language")
             val  (get %2 "@value")]
         (when-not (and lang val)
           (throw (ex-info (str "Parsing language item but does not contain both @language and @value: " language-items)
                           {:status 400 :error :db/invalid-context})))
         (assoc %1 (keyword lang) val))
      {} language-items*)))


(defn flatten-language-items
  "Takes a multi-lingual property makes it a standard string using the preferred
  language(s)"
  [language-items lang-prefs]
  (let [parsed    (parse-language-items language-items lang-prefs)
        ;; iterate over lang-prefs and pick the first match found
        preferred (some #(get parsed %) lang-prefs)]
    ;; if no matches found, just pick the one an log out a message
    (if preferred
      preferred
      (let [picked (first parsed)]
        (log/info (str "No language preference found for multi-language property, picking: "
                       (key picked) "from options: " parsed))
        (val picked)))))


(defn expand-property
  "Expands the property (key in context map) with a full IRI.
  Converts some special IRIs into keywords, like :rdfs/subClassOf and :rdfs/comment"
  [property context]
  (let [k* (iri-util/expand property context)]
    (cond
      (str/starts-with? k* "http://www.w3.org/2000/01/rdf-schema#")
      (keyword "rdfs" (subs k* (count "http://www.w3.org/2000/01/rdf-schema#")))

      (= "http://www.w3.org/2000/01/rdf-schema#subClassOf" k*)
      :rdfs/subClassOf

      (= "http://www.w3.org/2000/01/rdf-schema#comment" k*)
      :rdfs/comment

      (= "http://www.w3.org/2000/01/rdf-schema#label" k*)
      :rdfs/label

      :else
      k*)))

(defn blank-iri?
  "Returns true if iri string is a blanl node identifier"
  [iri]
  (and (string? iri)
       (str/starts-with? iri "_:")))

(defn blank-node?
  "Returns true if property key is a blank node (starts with '_:.....'"
  [item]
  (blank-iri? (get item "@id")))


;; TODO - see if same or can combine with normalize-context above
(defn expand-item
  "Expands a single item inside of a json-ld @graph"
  [item context base-iri {:keys [lang-props lang-prefs lang-flatten ignore] :as opts}]
  (try*
    (let [item-context (if-let [ctx-i (get item "@context")]
                         (iri-util/expanded-context ctx-i context)
                         context)
          [iri id] (when-let [id (get item "@id")]
                     (let [iri (iri-util/expand id item-context)]
                       [iri (remove-base-iri iri base-iri)]))
          type         (some->> (get item "@type")
                                sequential
                                (mapv #(iri-util/expand % item-context)))
          base         (cond-> {}
                               id (assoc :id id
                                         :iri iri)
                               type (assoc :type type))]
      (reduce-kv (fn [acc k v]
                   (let [k*      (expand-property k item-context)
                         ignore? (or (#{"@id" "@type" "@context"} k*)
                                     (some #(str/starts-with? k* %) ignore))
                         v*      (cond
                                   ignore? nil
                                   (lang-flatten k*) (flatten-language-items v lang-prefs)
                                   (lang-props k*) (parse-language-items v lang-prefs)
                                   (= :rdfs/subClassOf k*) (->> (mapv #(expand-item % item-context base-iri opts) (sequential v))
                                                                (filterv #(not (blank-iri? (:iri %)))))
                                   (= "@list" k) (mapv #(expand-item % item-context base-iri opts) v)
                                   (= \@ (first k)) v
                                   (map? v) (expand-item v item-context base-iri opts)
                                   (sequential? v) (mapv #(expand-item % item-context base-iri opts) v)
                                   :else (iri-util/expand v item-context))]
                     (if ignore?
                       acc
                       (assoc acc k* v*))))
                 base item))
    (catch* e
            (log/error e (str "Error parsing json-ld item: " item))
            (throw e))))


(defn within-ontology?
  "Returns true if the parsed ontology item is within the ontology
  as defined by the base-iri."
  [item base-iri]
  (and (string? (:iri item))
       (str/starts-with? (:iri item) base-iri)))


(defn expand-graph
  "Expands a JSON-LD document. Either can be a map with a @context and @graph key, or
   can be a vector of elements (just the @graph part).

   If a base-iri is included, the map will include not the full iri, but the iri excluding the
   base."
  ([json-ld] (expand-graph json-ld nil nil))
  ([json-ld base-iri opts]
   (let [map?     (map? json-ld)
         context  (when map?
                    (get json-ld "@context"))
         graph    (if map?
                    (get json-ld "@graph")
                    json-ld)
         context* (if context
                    (iri-util/expanded-context context)
                    {})]                                    ;; add trailing slash to base-iri for matching/removing
     (reduce (fn [acc item]
               (let [item* (expand-item item context* base-iri opts)]
                 (if (within-ontology? item* base-iri)      ;; ignore definitions not within the base-iri
                   (assoc acc (:id item*) item*)
                   acc)))
             {} graph))))


(defn strip-base-iri
  "Removes leading protocol (i.e. http:// or https://) in addition to
  trailing '/' or '#' on a base-iri. Used to look up ontology stored on disk
  using a file system path."
  [base-iri]
  (let [without-protocol (second (str/split base-iri #"://")) ;; remove i.e. http://, or https://
        trailing-char?   (#{\/ \#} (last without-protocol))]
    (if trailing-char?
      (subs without-protocol 0 (dec (count without-protocol)))
      without-protocol)))


(defn load-ontology
  ([base-iri] (load-ontology base-iri "ontologies/"))
  ([base-iri ontology-dir]
   (let [ontology-path (str ontology-dir (strip-base-iri base-iri) ".json")]
     (some-> ontology-path
             io/resource
             slurp
             (json/parse false)))))


(defn save-context
  [context filename]
  (->> (pprint/pprint context)
       with-out-str
       (spit filename)))


(defn class-type-to-set
  "Takes a class type from options and converts it into a predicate function
  class types can be a string, vector, or set. Turns all of them into a set."
  [class-types]
  (cond
    (string? class-types) #{class-types}
    (set? class-types) class-types
    (sequential? class-types) (into #{} class-types)
    (nil? class-types) #{}))


(defn is-class?
  [item opts]
  (let [class-types (or (:class-types opts)
                        "http://www.w3.org/2000/01/rdf-schema#Class")
        class-check (class-type-to-set class-types)]
    (some class-check (:type item))))


(defn is-property?
  [item opts]
  (let [prop-types     (or (:property-types opts)
                           "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property")
        property-check (class-type-to-set prop-types)]
    (some property-check (:type item))))


(defn is-data-type?
  "Note that data-type should be checked before is-class?, as the data types
  i.e. https://schema.org/Text, have two types defined - DataType and Class:
  {:iri 'https://schema.org/Text'
   :type ['http://www.w3.org/2000/01/rdf-schema#Class' 'https://schema.org/DataType']
   ...}"
  [item opts]
  (let [data-types (or (:data-types opts)
                       "https://schema.org/DataType")
        data-check (class-type-to-set data-types)]
    (some data-check (:type item))))


(defn item-type
  "Returns one of the possible 4 item types in schema.org we care about.
  - :data-type - i.e. Text, DateTime, etc. We want to ignore (remove) these as we use our internal data types.
  - :class - A class, i.e. Person
  - :property - A property, i.e. 'name'
  - :member - An instance/member of a class. In schema.org these only consist of Enum values, i.e. Male/Female"
  [item opts]
  (cond
    (is-data-type? item opts) :data-type
    (is-class? item opts) :class
    (is-property? item opts) :property
    :else :member))


(defn is-superseded?
  "schema.org has properties that have been superseded by other properties
  and don't have sufficent info for data type with in them. We drop them from the schema."
  [item opts]
  (let [superseded-prop (or (:superseded-prop opts)
                            "https://schema.org/supersededBy")]
    (contains? item superseded-prop)))


(defn- shorten-iri
  "If an IRI starts with https://schema.org/, removes so only left with
  unique ID. This works because no properties in schema.org have deeper
  nested namespaces, if that ever changes this logic will need to be changed."
  [iri base-iri]
  (if (str/starts-with? iri base-iri)
    (subs iri (count base-iri))
    iri))


(defn is-enum?
  [item ctx opts]
  (let [enum-class (or (:enum-class opts)
                       "https://schema.org/Enumeration")
        parents    (:rdfs/subClassOf item)]
    (if (some #(= enum-class (:iri %)) parents)             ;; if iri of :rdfs/subClassOf is equal to enum-class it is an enum
      true
      ;; if not an enum, but has parents, it is possible its parents are enums which makes this an enum.
      (some->> (not-empty parents)
               (keep #(get ctx (:id %)))
               (some #(is-enum? % ctx opts))))))


(defn enrich-classes
  "Finds parent types so they can be generated"
  [item base-iri ctx opts]
  (let [parent-type nil #_(->> (:type item)
                               (filter #(not= "http://www.w3.org/2000/01/rdf-schema#Class" %))
                               (mapv #(shorten-iri % base-iri)))
        enum?       (is-enum? item ctx opts)]
    #_(when (not-empty parent-type)
        (log/warn "Parent type: " parent-type item))
    (cond-> (assoc item :class? true)
            enum? (assoc :enum? true))))


(defn scalar-warnings
  "Logs warning for specific data types that are generic, so we pick one."
  [scalar-type fluree-type]
  (let [generic-types #{"https://schema.org/Number" "http://www.w3.org/2000/01/rdf-schema#Literal"}]
    (when (generic-types scalar-type)
      (log/info "Generic data type" scalar-type "is being set to the Fluree data type of" fluree-type))))


(def scalar->type {"http://www.w3.org/2000/01/rdf-schema#Literal" :string ;; note this is the parent class of all literals, choose string but ideally should override
                   "http://www.w3.org/2001/XMLSchema#string"      :string
                   "http://www.w3.org/2001/XMLSchema#boolean"     :boolean
                   "https://schema.org/Text"                      :string
                   "https://schema.org/Boolean"                   :boolean
                   "https://schema.org/URL"                       :uri

                   ;; numbers
                   "http://www.w3.org/2001/XMLSchema#float"       :float
                   "http://www.w3.org/2001/XMLSchema#double"      :double
                   "http://www.w3.org/2001/XMLSchema#decimal"     :double
                   "http://www.w3.org/2001/XMLSchema#int"         :long
                   "https://schema.org/Number"                    :double
                   "https://schema.org/Integer"                   :long
                   "https://schema.org/Float"                     :double

                   ;; date/time
                   "http://www.w3.org/2001/XMLSchema#dateTime"    :dateTime
                   "http://www.w3.org/2001/XMLSchema#duration"    :duration
                   "http://www.w3.org/2001/XMLSchema#time"        :time
                   "http://www.w3.org/2001/XMLSchema#date"        :date
                   "https://schema.org/Date"                      :date
                   "https://schema.org/Time"                      :time
                   "https://schema.org/DateTime"                  :dateTime})


(defn scalar?
  "Returns true if the type iri is a scalar we recognize."
  [type-iri]
  (contains? scalar->type type-iri))


(defn scalar->fluree-type
  "Converts a scalar type to its respective Fluree type. Some very general scalars,
  i.e. Number, get a specific type so we log those choices out just to make user aware."
  [scalar-type]
  (let [fluree-type (get scalar->type scalar-type)]
    (scalar-warnings scalar-type fluree-type)
    fluree-type))


(defn pick-scalar
  "When multiple scalar types are defined for a single property, pick them in a
  priority order that allows the most flexible data type to the most restrictive
  as defined here."
  [range-includes]
  (let [priority [:string :uri :dateTime :date :time :double :int :boolean]
        types    (->> range-includes
                      (map :iri)
                      (map scalar->fluree-type)
                      (into #{}))]
    (if (and (contains? types nil) (> (count types) 1))
      (do (log/info (str "Range-includes values contains a mix of scalar types and refs. Choosing a ref. "
                         "Types are: " range-includes))
          nil)
      (some types priority))))


(defn local-class?
  "returns true if an item referred in, i.e. rangeIncludes, refers to another class
  inside this vocabularly. Assumes item passed in as a map with both an :id and :iri key,
  the :id will be the shortened IRI which should have a key entry in the context we are generating
  if it is a class that is local"
  [context ref]
  (contains? context (:id ref)))


(defn restrict-collections
  [item range-includes ctx {:keys [range-includes-ignore] :as opts}]
  (let [{:keys [local-class scalar other]} (group-by #(cond
                                                        (scalar? %) :scalar
                                                        (local-class? ctx %) :local-class
                                                        :else :other)
                                                     range-includes)
        ;; schema.org includes top level "Thing", which we want to ignore as it would restrict
        ;; a collection to everything, which isn't the intent.

        thing? (some #(= "Thing" %) local-class)]
    #_(when other
        (throw (ex-info (str "Detected unknown types for predicate in rangeIncludes for item: " item)
                        {:status 400
                         :error  :db/invalid-context})))
    (when (and (empty? local-class) (empty? other))
      (throw (ex-info (str "No classes for predicate specified in rangeIncludes for item: " item)
                      {:status 400
                       :error  :db/invalid-context})))
    (not-empty
      (filterv #(not (range-includes-ignore (:iri %))) range-includes))))


(defn enrich-range-includes
  "Only does this for properties/predicates, not classes"
  [item base-ctx opts]
  (let [range-includes-prop  (or (:range-includes-prop opts)
                                 [:rdfs/range
                                  "https://schema.org/rangeIncludes"])
        range-includes       (sequential (some #(get item %) range-includes-prop))
        _                    (when-not range-includes
                               (throw (ex-info (str "Range Includes error, no " range-includes-prop
                                                    " for: " (:id item))
                                               item)))
        scalar-type          (pick-scalar range-includes)
        type                 (or scalar-type :ref)
        restrict-collections (when (= :ref type)
                               (restrict-collections item range-includes base-ctx opts))
        item*                (-> item
                                 (assoc :fluree/type type)
                                 (dissoc "https://schema.org/rangeIncludes"))]
    (cond-> item*
            restrict-collections (assoc :fluree/restrictCollection restrict-collections))))


(defn is-enum?
  [item ctx opts]
  (let [enum-class (or (:enum-class opts)
                       "https://schema.org/Enumeration")
        parents    (:rdfs/subClassOf item)]
    (if (some #(= enum-class (:iri %)) parents)             ;; if iri of :rdfs/subClassOf is equal to enum-class it is an enum
      true
      ;; if not an enum, but has parents, it is possible its parents are enums which makes this an enum.
      (some->> (not-empty parents)
               (keep #(get ctx (:id %)))
               (some #(is-enum? % ctx opts))))))


(defn enrich-member
  "Enriches member/instance items within the schema. Currently only adds :enum? true/false.

  If an instance/member of a class that is of enum, flags as an enum.

  i.e. https://schema.org/Female is a member of class https://schema.org/GenderType
  which is a subclass of https://schema.org/Enumeration. Therefore the class GenderType
  is intended to hold enum values, and therefore members/instances of GenderType (i.e. Female)
  is an enum value. Note that while GenderType is directly a subclass of Enumeration, there
  could be additional parents inbetween. This will crawl the class hierarchy to the top
  to ensure it does not find any parent of Enumeration type."
  [item base-iri base-ctx opts]
  (let [enum? (->> (:type item)
                   (map #(shorten-iri % base-iri))
                   (map #(get base-ctx %))
                   (some #(is-enum? % base-ctx opts)))]
    (if enum?
      (assoc item :enum? true)
      item)))


(defn enrich
  [base-context base-iri opts]
  (reduce-kv
    (fn [acc k item]
      (let [type (item-type item opts)]
        (if (or (= :data-type type)                         ;; we drop all data type definitions (Text, Boolean, etc.) as we have internal typing and they shouldn't be directly used in transactions for data.
                (is-superseded? item opts))                 ;; superseded by properties skipped
          acc
          (assoc acc k (cond-> item
                               (= :class type) (enrich-classes base-iri base-context opts)
                               (= :property type) (enrich-range-includes base-context opts)
                               (= :member type) (enrich-member base-iri base-context opts))))))
    {} base-context))

(defn onotology->context
  "Ontology conversion into a fluree-formatted edn context.
  Opts include:
   - :ignore - vector of URL strings that if match the beginning of a property via str/starts
               it will not include it in the final output
   - :lang-prefs - a vector of preferred languages as keywords i.e. [:en :en-US]. Used to pick
                       a language string when converting a property from multi-language ->
                       single language.
                       When parsing a multi-language field if the value is not already multi-
                       lingual and just a string value, will use the first element in this list
                       (i.e. :en in above example) as the assumed language and convert into
                       a language field.
   - :lang-props - a set of properties that are multi-lingual and parsed as such. This
                       should be every multi-lingual property in the ontology regardless of if
                       you want it multi-lingual in the result or not (that is set next)
   - :lang-flatten - a set of properties that are multi-lingual (not necessary to also be in :lang-props as assumed to be a lang-prop)
                     that we want flatted into a single string based on :lang-prefs order. If
                     no language string exists for any of the :lang-prefs it will just pick the
                     first language string in the list. i.e. if both :rdfs/label and :rdfs/comment
                     are multi-lingual in the ontology, :lang-props would be #{:rdfs/label :rdfs/comment}
                     and if we wanted :rdfs/comment to be 'flattened' and only come out as the 'en' string
                     it's value would be #{:rdfs/comment}
   - :range-includes-prop - vector of properties that define range includes. Default is:
                     ['http://www.w3.org/2000/01/rdf-schema#range', 'https://schema.org/rangeIncludes']
                     which covers standard RDF and schema.org. Items are checked for the first one found,
                     if multiple exist on a single item it will ignore subsequent ones (should never happen)
   - :range-includes-ignore - set of range items to ignore. 'range includes' for predicates end up with restrictCollection, but sometimes a
                     vocabulary puts a very generic/top-level item into the range and we don't want to ignore
                     it as opposed make it a restriction. Defaults to:
                     #{http://www.w3.org/2000/01/rdf-schema#Resource https://schema.org/Thing}
   - :enum-class - if the vocabulary defines an enum class (i.e. https://schema.org/Enumeration)
                   will mark predicate as :enum? true. Checks parent classes for enum, as a parent with enum
                   will mean all children are also enum.
   - :merge - map of key/vals to merge into ontology once complete. These are the over-rides to add, correct
              or more accurately specify things in the vocabulary. Merges with merge-in."
  ([ontology base-iri] (onotology->context ontology base-iri nil))
  ([ontology base-iri opts]
   (let [default-opts {:enum-class            "https://schema.org/Enumeration"
                       :range-includes-prop   [:rdfs/range
                                               "https://schema.org/rangeIncludes"]
                       :range-includes-ignore #{"http://www.w3.org/2000/01/rdf-schema#Resource"
                                                "https://schema.org/Thing"}
                       :ignore                nil
                       :lang-prefs            [:en :en-US :en-GB]
                       :lang-props            #{:rdfs/label :rdfs/comment}
                       :lang-flatten          #{:rdfs/comment}}
         opts*        (merge default-opts opts)]
     (-> (expand-graph ontology base-iri opts*)
         (enrich base-iri opts*)
         (#(merge-with merge % (:merge opts)))))))