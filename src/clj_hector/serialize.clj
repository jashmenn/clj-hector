(ns clj-hector.serialize
  (:import
   [me.prettyprint.cassandra.serializers AsciiSerializer
    BigIntegerSerializer BooleanSerializer ByteBufferSerializer
    BytesArraySerializer CharSerializer CompositeSerializer
    DateSerializer DoubleSerializer DynamicCompositeSerializer
    FastInfosetSerializer FloatSerializer IntegerSerializer
    JaxbSerializer LongSerializer ObjectSerializer
    PrefixedSerializer SerializerTypeInferer ShortSerializer
    StringSerializer TypeInferringSerializer UUIDSerializer]
   [me.prettyprint.cassandra.model QueryResultImpl
    HColumnImpl ColumnSliceImpl RowImpl RowsImpl
    SuperRowImpl SuperRowsImpl HSuperColumnImpl]))

(defprotocol ToClojure
  (to-clojure [x] "Convert hector types to Clojure data structures"))

(extend-protocol ToClojure
  SuperRowsImpl
  (to-clojure [s]
              (map to-clojure (iterator-seq (.iterator s))))
  SuperRowImpl
  (to-clojure [s]
              {(.getKey s) (map to-clojure (seq (.. s getSuperSlice getSuperColumns)))})
  HSuperColumnImpl
  (to-clojure [s]
              {(.getName s) (into (hash-map) (for [c (.getColumns s)] (to-clojure c)))})
  RowsImpl
  (to-clojure [s]
              (map to-clojure (iterator-seq (.iterator s))))
  RowImpl
  (to-clojure [s]
              {(.getKey s) (to-clojure (.getColumnSlice s))})
  ColumnSliceImpl
  (to-clojure [s]
              (into (hash-map) (for [c (.getColumns s)] (to-clojure c))))
  HColumnImpl
  (to-clojure [s]
              {(.getName s) (.getValue s)})
  Integer
  (to-clojure [s]
              {:count s})
  QueryResultImpl
  (to-clojure [s]
              (with-meta (to-clojure (.get s)) {:exec_us (.getExecutionTimeMicro s)
                                                :host (.getHostUsed s)})))

(def *serializers* {:integer (IntegerSerializer/get)
                    :string (StringSerializer/get)
                    :long (LongSerializer/get)
                    :bytes (BytesArraySerializer/get)
                    :infer (TypeInferringSerializer/get)
                    :ascii (AsciiSerializer/get)
                    :bigint (BigIntegerSerializer/get)
                    :boolean (BooleanSerializer/get)
                    :byte-buffer (ByteBufferSerializer/get)
                    :char (CharSerializer/get)
                    :date (DateSerializer/get)
                    :double (DoubleSerializer/get)
                    :fast-infoset (FastInfosetSerializer/get)
                    :float (FloatSerializer/get)
                    :object (ObjectSerializer/get)
                    :short (ShortSerializer/get)
                    :uuid (UUIDSerializer/get)})

(defn serializer
  "Returns serialiser based on type of item"
  [x]
  (if (keyword? x)
    (x *serializers*)
    (SerializerTypeInferer/getSerializer x)))
