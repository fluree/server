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
  (iri-for [_ prefix]
    (get namespaces prefix))
  (get-namespaces [_]
    (dissoc namespaces :base))
  (get-base [_]
    (:base namespaces))
  (new-qname [_ prefix local]
    (exec.where/match-iri (str (get namespaces prefix) local)))
  (new-iri [_ iri]
    iri)
  (new-literal [_ s]
    (-> exec.where/unmatched
        (exec.where/match-value s)))
  (new-literal [_ s t]
    (let [datatype (-> t ::exec.where/iri)]
      (-> exec.where/unmatched
          (exec.where/match-value s datatype))))
  (new-lang-string [_ s lang]
    (-> exec.where/unmatched
        (exec.where/match-lang s lang)))
  (rdf-type [_]
    RDF-TYPE)
  (rdf-first [_] RDF-FIRST)
  (rdf-rest [_] RDF-REST)
  (rdf-nil [_] RDF-NIL))

(defn new-generator
  []
  (->Generator 0 {} {}))

(defn parse
  [ttl]
  (let [gen (new-generator)]
    (-> ttl
        (raphael/parse gen)
        :triples)))
