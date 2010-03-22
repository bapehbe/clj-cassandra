(ns cassandra.client
  (:use [cassandra.internal])
  (:import [org.apache.thrift.transport TSocket]
	   [org.apache.thrift.protocol TBinaryProtocol]
	   [org.apache.cassandra.thrift Cassandra$Client ColumnPath SuperColumn
	    Column Mutation ColumnOrSuperColumn ColumnParent SlicePredicate]))

(defn mk-client
  [host port]
  (let [t (TSocket. host port)
        p (TBinaryProtocol. t)]
    (do (.open t)
	(proxy [Cassandra$Client] [p]
	  (transport [] t)
	  (close [] (.close t))))))

(defn mk-keyspace
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
  [ks cf-name]
  (assoc ks :cf cf-name))
  
(defn keyspaces
  [#^Cassandra$Client client]
  (set (.describe_keyspaces client)))

(defn cluster-name
  [#^Cassandra$Client client]
  (.describe_cluster_name client))

(defn version
  [#^Cassandra$Client client]
  (.describe_version client))

(defn space-info
  [ks-spec]
  (let [{:keys [#^Cassandra$Client client name]} ks-spec
	cfs (into {} (.describe_keyspace client name))]
    (reduce (fn [rst [k v]] (assoc rst k (into {} v))) {} cfs)))

(defn get-attr
  ([cf pk col]
     (get-val cf pk nil col))
  ([{:keys [#^Cassandra$Client client cf name encoder decoder read-level]}
    pk super col]
     (let [cp (column-path encoder cf super col)
	   column (.get client name pk cp read-level)]
       (second (extract-csc column decoder)))))

(defn get-slice*
  [cf-spec pk slice-pred]
  (let [{:keys [#^Cassandra$Client client cf name encoder decoder read-level]} cf-spec
	cp (column-parent encoder cf nil)
	cscs (.get_slice client name pk cp slice-pred read-level)]
    (into {} (map #(extract-csc % decoder) cscs))))

(defn get-slice-by-names
  [cf-spec pk & attr-names]
  (let [{encoder :encoder} cf-spec
	sp (mk-names-pred encoder attr-names)]
    (get-slice* cf-spec pk sp)))

(defn get-keys-attrs
  [cf-spec pks attr-names]
  (let [{:keys [#^Cassandra$Client client encoder decoder read-level name cf]} cf-spec
	sp (mk-slice-pred encoder attr-names)
	cp (column-parent encoder cf nil)
	rst (.multiget_slice client name pks cp sp read-level)]
    (into {} (for [[pk cscs] rst]
	       [pk (into {} (map #(extract-csc % decoder) cscs))]))))

(defn get-attrs
  [cf-spec pk & range-spec]
  (get-slice* cf-spec pk (apply mk-slice-range range-spec)))

(defn set-attr!
  ([cf-spec pk col val]
     (set-attr! cf-spec pk nil col val))
  ([cf-spec pk super col val]
     (let [{:keys [#^Cassandra$Client client encoder name cf write-level]} cf-spec
	   timestamp (now)
	   val (encoder val)
	   cp (column-path encoder cf super col)]
       (.insert client name pk cp val timestamp write-level))))

(defn batch-mutate!
  [cf-spec pk mutations]
  (let [{:keys [#^Cassandra$Client client name cf write-level]} cf-spec
	muts {pk {cf (doall mutations)}}]
    (.batch_mutate client name muts write-level)))

(defn set-attrs!
  [cf-spec pk attrs]
  (let [{encoder :encoder} cf-spec
	timestamp (now)
	mutations (for [[k v] attrs]
		    (mk-mutation k v encoder timestamp))]
    (batch-mutate! cf-spec pk mutations)))

