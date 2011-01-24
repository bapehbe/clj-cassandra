(ns cassandra.client-test
  "Tests which need a cassandra setup. Using the immediate
   Cassandra downloaded configuration to test."
  (:use [cassandra.client :reload-all true]
	[clojure.test]))

(def table (-> (mk-client "localhost" 9160)
	       (key-space "Keyspace1")
	       (column-family "Standard1")))

(defn clean-foo [t]
  (t)
  (remove-attr! table "foo"))

(use-fixtures :each clean-foo)

(deftest set-get-cycle
  (testing "Single attribute get and set cycle"
    (are [attr-name attr-value]
	 (= (do
	      (set-attr! table "foo" attr-name attr-value)
	      (get-attr table "foo" attr-name)) attr-value)
	 :hello "world!"
	 (java.util.UUID/fromString "36089f00-4b91-11df-86ef-0685b7ddb76d") "ok"
	 :ok {12 :foo, "35" 4.5}))
  (testing "Multiple attributes get and set"
    (are [attrs]
	 (= (do
	      (set-attrs! table "foo" attrs)
	      (get-attrs table "foo" {:column-names (keys attrs)}))
	    attrs)
	 {:foo {:bar "ok", "3" 5}}
	 {:hello "world", 1 [3 5]})))

(deftest fecthing-nothing
  (is (nil? (get-attr table "foo" "not-exist"))))

(deftest adding-collection
  (testing "Add collection of attributes into the database"
    (are [coll]
	 (= (do
	      (add-collection! table "foo" coll)
	      (vals (get-collection table "foo")))
	    coll)
	 [{:hello "world", :ok ":-)"}
	  {:hello "moon", :ok ":-("}])))

;http://www.datastax.com/blog/whats-new-cassandra-07-secondary-indexes
;create column family users with comparator=UTF8Type 
;  and column_metadata=[{column_name: full_name, validation_class: UTF8Type},
;  {column_name: birth_date, validation_class: LongType, index_type: KEYS},
;  {column_name: state, validation_class: UTF8Type, index_type: KEYS}];
(deftest test-query-indexed
   (with-open [client (mk-client "localhost" 9160)]
     (let [table (-> client (key-space "demo") (column-family "users"))]
       (set-attrs! table 'bsanderson {'full_name "Brandon Sanderson" 'state "UT"})
       (set-attrs! table 'prothfuss {'full_name "Patrick Rothfuss" 'state "WI"})
       (set-attrs! table 'htayler {'full_name "Howard Tayler" 'state "UT"})
       (is (= 2 (count 
                  (query-indexed table '[(= state "UT")] {}))))
       (remove-attr! table 'bsanderson)
       (remove-attr! table 'prothfuss)
       (remove-attr! table 'htayler)
       (is (= 0 (count 
                  (query-indexed table '[(= state "UT")] {})))))))
