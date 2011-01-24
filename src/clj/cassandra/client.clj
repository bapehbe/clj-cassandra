(ns cassandra.client
  "The public client interfaces for Cassandra"
  (:use [cassandra.internal])
  (:import [org.apache.thrift.transport TFramedTransport TSocket]
	   [org.apache.thrift.protocol TBinaryProtocol]
	   [org.apache.cassandra.thrift Cassandra$Client ColumnPath SuperColumn
	    Column Mutation ColumnOrSuperColumn ColumnParent SlicePredicate NotFoundException]
	   [cassandra TimeUUID]))

(defn mk-client
  "Make a closeable Cassandra client connection to the db.
   You may use it in a with-open statement.
   TODO for most server applications, we need connection pool,
   I am working on a generic connection pool."
  [host port]
  (let [s (TSocket. host port)
        t (TFramedTransport. s)
        p (TBinaryProtocol. t)]
    (.open t)
    (proxy [Cassandra$Client java.io.Closeable] [p]
      (close [] (.close t)))))

(defn key-space
  "Make a client connection and keyspace-name to specify a keyspace.
   Options include: :decoder and :encoder to override the default encode/decode function.
   r-level and w-level for read-level and write-level of the database."
  [client keyspace-name & [options]]
  (let [decoder (get options :decoder decode)
	encoder (get options :encoder encode)
	key-decoder (get options :key-decoder decode)
	r-level (get options :read-level :quorum)
	w-level (get options :write-level :quorum)]
    (.set_keyspace client keyspace-name)
    {:client client
     :name keyspace-name
     :encoder encoder
     :decoder decoder
     :read-level (translate-level r-level)
     :write-level (translate-level w-level)}))

(defn column-family
  "Use a keyspace specification ks and column family name cf-name to 
   specify a column family (aka database table)"
  [ks cf-name]
  (assoc ks :cf cf-name))

(defn mk-cf-spec
  "Make a column-family specification in one shot."
  [host port key-space-name column-family-name & [options]]
  (-> (mk-client host port)
      (key-space key-space-name options)
      (column-family column-family-name)))
  
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
     (try
      (let [cp (column-path encoder cf super col)
	    column (.. client (get (encode pk) cp read-level) (getColumn))]
	(decoder (.getValue column)))
      (catch NotFoundException e nil))))
  

(defn get-slice*
  "Get attributes of predicates of slice-pred of pk.
   Use internally."
  [cf-spec pk slice-pred]
  (let [{:keys [#^Cassandra$Client client cf name encoder key-decoder decoder read-level]} cf-spec
	key-decoder (if key-decoder key-decoder decoder)
	cp (column-parent encoder cf nil)
	cscs (.get_slice client (encoder pk) cp slice-pred read-level)]
    (apply array-map (mapcat #(extract-csc % key-decoder decoder) cscs))))

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
	sp (mk-names-pred encoder attr-names)
	cp (column-parent encoder cf nil)
	rst (.multiget_slice client name pks cp sp read-level)]
    (into {} (for [[pk cscs] rst]
	       [pk (into {} (map #(extract-csc % decoder) cscs))]))))

(defn get-attrs
  "Get attributes of pk."
  ([cf-spec primary-key]
     (get-attrs cf-spec primary-key {}))
  ([cf-spec primary-key attr-spec]
     (let [{encoder :encoder} cf-spec
	   {:keys [column-names name-start name-end count reverse]} attr-spec]
       (get-slice* cf-spec primary-key (mk-slice-pred encoder attr-spec)))))

(defn set-attr!
  "Set the attribute of column col of pk in cf-spec"
  ([cf-spec pk col val]
     (set-attr! cf-spec pk nil col val))
  ([cf-spec pk super col val]
     (let [{:keys [#^Cassandra$Client client encoder name cf write-level]} cf-spec
	   timestamp (now)
	   column (kv-to-column encoder col val timestamp)
	   parent (column-parent encoder cf super)]
       (.insert client (encoder pk) parent column write-level))))

(defn batch-mutate*
  "Persitent mutations of pk in cf-spec.
   Use internally."
  [cf-spec pk mutations]
  (let [{:keys [#^Cassandra$Client client encoder cf write-level]} cf-spec
	muts {(encoder pk) {cf (doall mutations)}}]
    (.batch_mutate client muts write-level)))

(defn set-attrs!
  "Set attributes attrs of pk in column family cf-spec.
   The attributes are specified by maps."
  [cf-spec pk attrs]
  (let [{encoder :encoder} cf-spec
	timestamp (now)
	mutations (for [[k v] attrs]
		    (mk-mutation k v encoder timestamp))]
    (batch-mutate* cf-spec pk mutations)))

(defn remove-attr!
  "Remove attributes or entire key"
  ([cf-spec primary-key]
     (remove-attr! cf-spec primary-key nil nil))
  ([cf-spec primary-key super-column column-name]
     (let [{:keys [#^Cassandra$Client client name cf encoder write-level]} cf-spec
	   cp (column-path encoder cf super-column column-name)]
       (.remove client (encoder primary-key) cp (now) write-level))))

(defn get-collection
  "Get attributes of primary-key from cf-spec, treat the key as Timed UUID."
  ([cf-spec primary-key]
     (get-collection cf-spec primary-key {}))
  ([cf-spec primary-key slice-spec]
     (get-attrs (assoc cf-spec :key-decoder uuid-decode) primary-key slice-spec)))

(defn add-collection!
  "Add element of coll to primary-key of cf-spec, with their key as Timed UUID,
   returns the generated uuids."
  [cf-spec primary-key coll]
  (let [attrs (into {} (map (fn [x] [(TimeUUID/getTimeUUID) x]) coll))]
    (set-attrs! cf-spec primary-key attrs)
    (keys attrs)))

(defn get-key-range
  [cf-spec key-range-spec slice-spec]
  (let [{:keys [name cf client encoder decoder read-level]} cf-spec
	parent (column-parent encoder cf nil)
	range (mk-key-range encoder key-range-spec)
	slice (mk-slice-pred encoder slice-spec)]
    (.get_range_slices client name parent slice range read-level)))

(defn query-indexed
  [cf-spec query attr-spec]
  (let [{:keys [name cf client encoder decoder read-level]} cf-spec
	parent (column-parent encoder cf nil)
        index_clause (clause query)
	slice (mk-slice-pred encoder attr-spec)
        slices (.get_indexed_slices client parent index_clause slice read-level)]
    (map #(keyslice-to-map % decoder) slices)))
