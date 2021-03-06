(ns clj-hector.ddl
  (:import [me.prettyprint.hector.api.factory HFactory]
           [me.prettyprint.hector.api Cluster]
           [me.prettyprint.cassandra.service ThriftCfDef]
           [me.prettyprint.hector.api.ddl ComparatorType ColumnFamilyDefinition ColumnType KeyspaceDefinition]))

(defn- make-column-family
  "Returns an object defining a new column family"
  ([keyspace column-family-name]
     (HFactory/createColumnFamilyDefinition keyspace column-family-name))
  ([keyspace column-family-name comparator-type]
     (let [cmp (condp = comparator-type
                   :ascii         ComparatorType/ASCIITYPE
                   :byte          ComparatorType/BYTESTYPE
                   :integer       ComparatorType/INTEGERTYPE
                   :lexical-uuid  ComparatorType/LEXICALUUIDTYPE
                   :long          ComparatorType/LONGTYPE
                   :time-uuid     ComparatorType/TIMEUUIDTYPE
                   :utf-8         ComparatorType/UTF8TYPE
                   (throw (Exception. "Unknown comparator type passed in column family definition")))]
       (HFactory/createColumnFamilyDefinition keyspace column-family-name cmp))))

(defn- make-keyspace-definition
  ([keyspace strategy-class replication-factor column-families]
     (let [column-families (map (fn [{:keys [name comparator type]}]
                                  (let [cf-def (if (nil? comparator)
                                                 (make-column-family keyspace name)
                                                 (make-column-family keyspace name comparator))]
                                    (if (nil? type)
                                      cf-def
                                      (doto ^ThriftCfDef cf-def
                                            (.setColumnType (if (= :super type)
                                                              ColumnType/SUPER
                                                              ColumnType/STANDARD))))))
                                column-families)]
       (HFactory/createKeyspaceDefinition keyspace
                                          strategy-class
                                          replication-factor
                                          column-families))))

(defn add-column-family
  "Adds a column family to a keyspace"
  ([^Cluster cluster keyspace {:keys [name comparator type]}]
     (let [cf (if (nil? comparator)
                (make-column-family keyspace name)
                (make-column-family keyspace name comparator))]
       (.addColumnFamily cluster (doto ^ThriftCfDef cf
                                   (.setColumnType (if (= :super type)
                                                     ColumnType/SUPER
                                                     ColumnType/STANDARD)))))))

(defn drop-column-family
  "Removes a column family from a keyspace"
  ([^Cluster cluster keyspace-name column-family-name]
     (.dropColumnFamily cluster keyspace-name column-family-name)))

(defn add-keyspace
  "Creates a new keyspace from the definition passed as a map"
  ([^Cluster cluster {:keys [name strategy replication column-families]}]
     (let [strategy (condp = strategy
                        :local            "org.apache.cassandra.locator.LocalStrategy"
                        :network-topology "org.apache.cassandra.locator.NetworkTopologyStrategy"
                        "org.apache.cassandra.locator.SimpleStrategy")
           replication (or replication 1)]
       (.addKeyspace cluster (make-keyspace-definition name
                                                       strategy
                                                       replication
                                                       column-families)))))

(defn drop-keyspace
  "Deletes a whole keyspace from the cluster"
  ([^Cluster cluster keyspace-name]
     (.dropKeyspace cluster keyspace-name)))

(defn keyspaces
  "Description of the keyspaces available in the Cassandra cluster"
  ([^Cluster cluster]
     (let [kss (.describeKeyspaces cluster)]
       (map (fn [^KeyspaceDefinition ks] {:name (.getName ks)
                                         :replication-factor (.getReplicationFactor ks)})
            kss))))

(defn- parse-type
  [x]
  (if (= ColumnType/SUPER x)
    :super
    :standard))

(defn- parse-comparator
  ([^ComparatorType comparator-type]
     (condp = (.getClassName comparator-type)
       "org.apache.cassandra.db.marshal.UTF8Type"        :utf-8
       "org.apache.cassandra.db.marshal.AsciiType"       :ascii
       "org.apache.cassandra.db.marshal.BytesType"       :byte
       "org.apache.cassandra.db.marshal.IntegerType"     :integer
       "org.apache.cassandra.db.marshal.LexicalUUIDType" :lexical-uuid
       "org.apache.cassandra.db.marshal.LongType"        :long
       "org.apache.cassandra.db.marshal.TimeUUIDType"    :time-uuid)))

(defn column-families
  "Returns all the column families for a certain keyspace"
  ([^Cluster cluster ^String keyspace]
     (let [ks (first (filter (fn [^KeyspaceDefinition ks] (= (.getName ks) keyspace))
                             (.describeKeyspaces cluster)))
           cf-defs (.getCfDefs ^KeyspaceDefinition ks)]
       (map (fn [^ColumnFamilyDefinition cf-def]
              {:name (.getName cf-def)
               :comparator (parse-comparator (.getComparatorType cf-def))
               :type (parse-type (.getColumnType cf-def))})
            cf-defs))))
