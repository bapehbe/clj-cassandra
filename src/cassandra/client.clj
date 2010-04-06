(ns cassandra.client
  "The public client interfaces for Cassandra"
  (:use [cassandra.internal])
  (:import [org.apache.thrift.transport TSocket]
	   [org.apache.thrift.protocol TBinaryProtocol]
	   [org.apache.cassandra.thrift Cassandra$Client ColumnPath SuperColumn
	    Column Mutation ColumnOrSuperColumn ColumnParent SlicePredicate]))

(defn mk-client
  "Make a closeable Cassandra client connection to the db.
   You may use it in a with-open statement.
   TODO for most server applications, we need connection pool,
   I am working on a generic connection pool."
  [host port]
  (let [t (TSocket. host port)
        p (TBinaryProtocol. t)]
    (do (.open t)
	(proxy [Cassandra$Client] [p]
	  (transport [] t)
	  (close [] (.close t))))))

(defn mk-keyspace
  "Make a client connection and keyspace-name to specify a keyspace.
   Options include: :decoder and :encoder to override the default encode/decode function.
   r-level and w-level for read-level and write-level of the database."
  [client keyspace-name & [options]]
  (let [decoder (get options :decoder decode)
	encoder (get options :encoder encode)
	r-level (get options :read-level :quorum)
	w-level (get options :write-level :quorum)]
    {:client client
     :name keyspace-name
     :encoder encoder
     :decoder decoder
     :read-level (translate-level r-level)
     :write-level (translate-level w-level)}))

(defn mk-cf
  "Use a keyspace specification ks and column family name cf-name to 
   specify a column family (aka database table)"
  [ks cf-name]
  (assoc ks :cf cf-name))
  
(defn keyspaces
  "List all keyspaces within a connected Cassandra cluster"
  [#^Cassandra$Client client]
  (set (.describe_keyspaces client)))

(defn cluster-name
  "Returns the cluster name of a client connection."
  [#^Cassandra$Client client]
  (.describe_cluster_name client))

(defn version
  "Returns the version of a cluster."
  [#^Cassandra$Client client]
  (.describe_version client))

(defn space-info
  "Returns information (column families) of keyspace ks-spec"
  [ks-spec]
  (let [{:keys [#^Cassandra$Client client name]} ks-spec
	cfs (into {} (.describe_keyspace client name))]
    (reduce (fn [rst [k v]] (assoc rst k (into {} v))) {} cfs)))

(defn get-attr
  "Returns a column's attribute of given pk in a column family cf.
   You can optional specify a super column super."
  ([cf pk column]
     (get-attr cf pk nil column))
  ([{:keys [#^Cassandra$Client client cf name encoder decoder read-level]}
    pk super col]
     (let [cp (column-path encoder cf super col)
	   column (.get client name pk cp read-level)]
       (second (extract-csc column decoder)))))

(defn get-slice*
  "Get attributes of predicates of slice-pred of pk.
   Use internally."
  [cf-spec pk slice-pred]
  (let [{:keys [#^Cassandra$Client client cf name encoder decoder read-level]} cf-spec
	cp (column-parent encoder cf nil)
	cscs (.get_slice client name pk cp slice-pred read-level)]
    (into {} (map #(extract-csc % decoder) cscs))))

(defn get-slice-by-names
  "Get attributes values of attr-names of pk in a column family cf-spec."
  [cf-spec pk & attr-names]
  (let [{encoder :encoder} cf-spec
	sp (mk-names-pred encoder attr-names)]
    (get-slice* cf-spec pk sp)))

(defn get-keys-attrs
  "Get attributes attr-names of multiple pks in column family cf-spec."
  [cf-spec pks attr-names]
  (let [{:keys [#^Cassandra$Client client encoder decoder read-level name cf]} cf-spec
	sp (mk-slice-pred encoder attr-names)
	cp (column-parent encoder cf nil)
	rst (.multiget_slice client name pks cp sp read-level)]
    (into {} (for [[pk cscs] rst]
	       [pk (into {} (map #(extract-csc % decoder) cscs))]))))

(defn get-attrs
  "Get attributes of a range specified by range-spec of pk in cf-spec."
  [cf-spec pk & range-spec]
  (get-slice* cf-spec pk (apply mk-slice-range range-spec)))

(defn set-attr!
  "Set the attribute of column col of pk in cf-spec"
  ([cf-spec pk col val]
     (set-attr! cf-spec pk nil col val))
  ([cf-spec pk super col val]
     (let [{:keys [#^Cassandra$Client client encoder name cf write-level]} cf-spec
	   timestamp (now)
	   val (encoder val)
	   cp (column-path encoder cf super col)]
       (.insert client name pk cp val timestamp write-level))))

(defn batch-mutate*
  "Persitent mutations of pk in cf-spec.
   Use internally."
  [cf-spec pk mutations]
  (let [{:keys [#^Cassandra$Client client name cf write-level]} cf-spec
	muts {pk {cf (doall mutations)}}]
    (.batch_mutate client name muts write-level)))

(defn set-attrs!
  "Set attributes attrs of pk in column family cf-spec.
   The attributes are specified by maps."
  [cf-spec pk attrs]
  (let [{encoder :encoder} cf-spec
	timestamp (now)
	mutations (for [[k v] attrs]
		    (mk-mutation k v encoder timestamp))]
    (batch-mutate* cf-spec pk mutations)))

