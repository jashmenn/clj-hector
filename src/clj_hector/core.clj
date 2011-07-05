(ns clj-hector.core
  (:require [clj-hector.serialize :as s])
  (:use [clojure.contrib.def :only (defnk)])
  (:import [java.io Closeable]
           [me.prettyprint.hector.api.mutation Mutator]
           [me.prettyprint.hector.api Cluster]
           [me.prettyprint.hector.api.factory HFactory]
           [me.prettyprint.hector.api.query Query]
           [me.prettyprint.cassandra.service CassandraHostConfigurator]
           [me.prettyprint.cassandra.serializers TypeInferringSerializer]))

;; following through sample usages on hector wiki
;; https://github.com/rantav/hector/wiki/User-Guide

(defn cluster
  "Connects to Cassandra cluster"
  ([cluster-name host]
     (cluster cluster-name host 9160))
  ([cluster-name host port]
     (HFactory/getOrCreateCluster cluster-name
                                  (CassandraHostConfigurator. (str host ":" port)))))
(defn keyspace
  [cluster name]
  (HFactory/createKeyspace name cluster))

(def type-inferring (TypeInferringSerializer/get))

(defnk create-column
  [n v :n-serializer type-inferring :v-serializer type-inferring :s-serializer type-inferring]
  (if (map? v)
    (let [cols (map (fn [[n v]] (create-column n v :n-serializer n-serializer :v-serializer v-serializer)) v)]
      (HFactory/createSuperColumn n cols s-serializer n-serializer v-serializer))
    (HFactory/createColumn n v n-serializer v-serializer)))

(defn put-row
  "Stores values in columns in map m against row key pk"
  [ks cf pk m]
  (let [^Mutator mut (HFactory/createMutator ks type-inferring)]
    (do (doseq [[k v] m] (.addInsertion mut pk cf (create-column k v)))
        (.execute mut))))

(defn- execute-query [^Query query]
  (s/to-clojure (.execute query)))

(defnk get-super-rows
  [ks cf pks sc :s-serializer :bytes :n-serializer :bytes :v-serializer :bytes :start nil :end nil :reverse false :limit Integer/MAX_VALUE]
  (execute-query (doto (HFactory/createMultigetSuperSliceQuery ks
                                                               (s/serializer (first pks))
                                                               (s/serializer s-serializer)
                                                               (s/serializer n-serializer)
                                                               (s/serializer v-serializer))
                   (.setColumnFamily cf)
                   (.setKeys (object-array pks))
                   (.setColumnNames (object-array sc))
                   (.setRange start end reverse limit))))

(defnk get-rows
  "In keyspace ks, retrieve rows for pks within column family cf."
  [ks cf pks :n-serializer :bytes :v-serializer :bytes :start nil :end nil :reverse false :limit Integer/MAX_VALUE]
  (execute-query (doto (HFactory/createMultigetSliceQuery ks
                                                          (s/serializer (first pks))
                                                          (s/serializer n-serializer)
                                                          (s/serializer v-serializer))
                   (.setColumnFamily cf)
                   (.setKeys (object-array pks))
                   (.setRange start end reverse limit))))

(defnk get-super-columns
  [ks cf pk sc c :s-serializer :bytes :n-serializer :bytes :v-serializer :bytes]
  (execute-query (doto (HFactory/createSubSliceQuery ks
                                                     type-inferring
                                                     (s/serializer s-serializer)
                                                     (s/serializer n-serializer)
                                                     (s/serializer v-serializer))
                   (.setColumnFamily cf)
                   (.setKey pk)
                   (.setSuperColumn sc)
                   (.setColumnNames (object-array c)))))

(defnk get-columns
  "In keyspace ks, retrieve c columns for row pk from column family cf"
  [ks cf pk c :n-serializer :bytes :v-serializer :bytes]
  (let [vs (s/serializer v-serializer)
        ns (s/serializer n-serializer)]
    (if (< 2 (count c))
      (execute-query (doto (HFactory/createColumnQuery ks type-inferring ns vs)
                       (.setColumnFamily cf)
                       (.setKey pk)
                       (.setName c)))
      (execute-query (doto (HFactory/createSliceQuery ks type-inferring ns vs)
                       (.setColumnFamily cf)
                       (.setKey pk)
                       (.setColumnNames (object-array c)))))))

(defn delete-columns
  [ks cf pk cs]
  (let [mut (HFactory/createMutator ks type-inferring)]
    (doseq [c cs] (.addDeletion mut pk cf c type-inferring))
    (.execute mut)))

(defnk delete-super-columns
  "Coll is a map of keys, super column names and column names

Example: {\"row-key\" {\"SuperCol\" [\"col-name\"]}}"
  [ks cf coll :s-serializer :bytes :n-serializer :bytes :v-serializer :bytes]
  (let [mut (HFactory/createMutator ks type-inferring)]
    (doseq [[k nv] coll]
      (doseq [[sc-name v] nv]
        (.addSubDelete mut k cf (create-column sc-name
                                               (apply hash-map (interleave v v))
                                               :s-serializer (s/serializer s-serializer)
                                               :n-serializer (s/serializer n-serializer)
                                               :v-serializer (s/serializer v-serializer)))))
    (.execute mut)))

(defn delete-rows
  "Deletes all columns for rows identified in pks sequence."
  [ks cf pks]
  (let [mut (HFactory/createMutator ks type-inferring)]
    (doseq [k pks] (.addDeletion mut k cf))
    (.execute mut)))

(defnk count-columns
  "Counts number of columns for pk in column family cf. The method is not O(1). It takes all the columns from disk to calculate the answer. The only benefit of the method is that you do not need to pull all the columns over Thrift interface to count them."
  [ks pk cf :start nil :end nil]
  (execute-query (doto (HFactory/createCountQuery ks
                                                  type-inferring
                                                  (s/serializer :bytes))
                   (.setKey pk)
                   (.setRange start end Integer/MAX_VALUE)
                   (.setColumnFamily cf))))


