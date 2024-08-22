(ns fluree.server.io.turtle
  (:require [fluree.db.query.exec.where :as exec.where]
            [quoll.raphael.core :as raphael]
            [quoll.rdf :as rdf :refer [RDF-TYPE RDF-FIRST RDF-REST RDF-NIL]]))

(set! *warn-on-reflection* true)

(defrecord Generator [counter bnode-cache namespaces]
  raphael/NodeGenerator
  (new-node [this]
    [(update this :counter inc) (rdf/unsafe-blank-node (str "b" counter))])
  (new-node [this label]
    (if-let [node (get bnode-cache label)]
      [this node]
      (let [node (rdf/unsafe-blank-node (str "b" counter))]
        [(-> this
             (update :counter inc)
             (update :bnode-cache assoc label node))
         node])))
  (add-base [this iri]
    (update this :namespaces assoc :base (rdf/as-str iri)))
  (add-prefix [this prefix iri]
    (update this :namespaces assoc prefix (rdf/as-str iri)))
  (iri-for [this prefix]
    (get namespaces prefix))
  (get-namespaces [this]
    (dissoc namespaces :base))
  (get-base [this]
    (:base namespaces))
  (new-qname [this prefix local]
    (exec.where/match-iri (str (get namespaces prefix) local)))
  (new-iri [this iri]
    iri)
  (new-literal [this s]
    (-> exec.where/unmatched
        (exec.where/match-value s)))
  (new-literal [this s t]
    (let [datatype (-> t ::exec.where/iri)]
      (-> exec.where/unmatched
          (exec.where/match-value s datatype))))
  (new-lang-string [this s lang]
    (-> exec.where/unmatched
        (exec.where/match-lang s lang)))
  (rdf-type [this]
    RDF-TYPE)
  (rdf-first [this] RDF-FIRST)
  (rdf-rest [this] RDF-REST)
  (rdf-nil [this] RDF-NIL))

(defn new-generator
  []
  (->Generator 0 {} {}))

(defn parse
  [ttl]
  (let [gen (new-generator)]
    (-> ttl
        (raphael/parse gen)
        :triples)))
