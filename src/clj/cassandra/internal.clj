(ns cassandra.internal
  "Represent Cassandra thrift structure by clojure data structure"
  (:import [org.apache.thrift.transport TSocket]
	   [org.apache.thrift.protocol TBinaryProtocol]
	   [org.apache.cassandra.thrift Cassandra$Client ColumnPath SuperColumn KeyRange
	    Column Mutation ColumnOrSuperColumn ColumnParent SlicePredicate SliceRange]
	   [java.nio ByteBuffer]
	   [java.util UUID]
	   [cassandra TimeUUID]))

(defn get-bytes
  [#^String s]
  (.getBytes s "UTF-8"))

(defn encode
  "Default clojure encoder"
  [data]
  (let [bytes (if (instance? UUID data)
                (TimeUUID/asByteArray data)
                (get-bytes (pr-str data)))]
    (ByteBuffer/wrap bytes)))

(defn bytes-decode [bytes-buf]
  (.array bytes-buf))

(defn decode
  "Default clojure decode"
  [bytes]
  (with-in-str (String. bytes "UTF-8") (read)))

(defn uuid-decode
  "Decode timed uuid"
  [bytes]
  (TimeUUID/toUUID bytes))

(defmacro translate-level
  "Translate level keywords to thrift.
   The available levels are:
     :zero :one :any :all :quorum :dcquorum :dcquorumsycn"
  [level]
  `(let [levels# {:zero 0, :one 1, :any 6, :all 5, :quorum 2, :dcquorum 3, :dcquorumsync 4}
	 lv# (get levels# ~level 1)]
     (org.apache.cassandra.thrift.ConsistencyLevel/findByValue lv#)))

(defn now
  []
  (System/currentTimeMillis))

(defn kv-to-column
  [encoder key val timestamp]
  (-> (Column.)
      (.setName (encoder key))
      (.setValue (encoder val))
      (.setTimestamp timestamp)))

(defn map-to-sc
  "Transfer a clojure map to supercolumn"
  [encoder key the-map timestamp]
  {:pre (map? the-map)}
  (let [cls (for [[k v] the-map]
	      (kv-to-column encoder k v timestamp))]
    (-> (SuperColumn.)
	(.setName (encoder key))
	(.setColumns (list cls)))))

(defn wrap-obj
  "wrap a object to column or supercolumn"
  [key val encoder timestamp]
  (let [#^ColumnOrSuperColumn rst (ColumnOrSuperColumn.)]
    ;REMIND should we support super column?
;    (if (map? val)
;      (.setSuper_column rst (map-to-sc encoder key val timestamp))
    (.setColumn rst (kv-to-column encoder key val timestamp))))

(defn sc-to-map
  [#^SuperColumn sc decoder]
  (let [cls (.getColumns sc)
	the-map (into {} (for [#^Column column cls]
			   [(decoder (.getName column))
			    (decoder (.getValue column))]))]
    the-map))
		  
(defn extract-csc
  ([csc decoder]
     (extract-csc csc decoder decoder))
  ([#^ColumnOrSuperColumn csc key-decoder val-decoder]
     (if (.isSetColumn csc)
       (let [#^Column column (.getColumn csc)]
	 [(key-decoder (.getName column)) (val-decoder (.getValue column))])
       (let [#^SuperColumn sc (.getSuper_column csc)]
	 [(key-decoder (.getName sc)) (sc-to-map sc)]))))
    
(defn column-path
  [encoder cf super name]
  (let [cf (str cf)
	cp (-> (ColumnPath.)
	       (.setColumn_family cf))]
    (when super
      (.setSuper_column cp (encoder super)))
    (when name
      (.setColumn cp (encoder name)))
    cp))

(defn column-parent
  [encoder cf super]
  (let [cf (str cf)
	cp (-> (ColumnParent.)
	       (.setColumn_family cf))]
    (when super
      (.setSuper_column cp (encoder super)))
    cp))

(defn mk-names-pred
  [encoder attr-names]
  (let [column-names (map encoder attr-names)]
    (-> (SlicePredicate.)
	(.setColumn_names column-names))))

(defn mk-range-pred
  ([]
     (mk-range-pred encode))
  ([encoder]
     (mk-range-pred encoder {}))
  ([encoder {:keys [name-start name-finish count reverse]
	     :or {name-start nil
		  name-finish nil
		  count 100
		  reverse false}}]
     (let [#^SliceRange range (SliceRange.)
	   conv #(if % (encoder %) (byte-array 0))
	   name-start (conv name-start)
	   name-finish (conv name-finish)
           range (-> range
		     (.setStart name-start)
		     (.setFinish name-finish)
		     (.setReversed reverse)
		     (.setCount count))
	   pred (SlicePredicate.)]
       (.setSlice_range pred range))))

(defn mk-slice-pred
  [encoder {column-names :column-names :as pred-spec}]
  (if column-names
    (mk-names-pred encoder column-names)
    (mk-range-pred encoder pred-spec)))

(defn mk-mutation
  [key val encoder timestamp]
  (let [clm (wrap-obj key val encoder timestamp)]
    (-> (Mutation.)
	(.setColumn_or_supercolumn clm))))

(defn mk-key-range
  [encoder {:keys [start-key end-key count]
 	    :or {count 100}}]
  (-> (KeyRange.)
      (.setStart_key start-key)
      (.setEnd_key end-key)
      (.setCount count)))
