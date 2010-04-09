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
  (remove-attr table "foo"))

(use-fixtures :each clean-foo)

(deftest set-get-cycle
  (testing "Single attribute get and set cycle"
    (are [attr-name attr-value]
	 (= (do
	      (set-attr! table "foo" attr-name attr-value)
	      (get-attr table "foo" attr-name)) attr-value)
	 :hello "world!"
	 :ok {12 :foo, "35" 4.5}))
  (testing "Multiple attributes get and set"
    (are [attrs]
	 (= (do
	      (set-attrs! table "foo" attrs)
	      (get-attrs table "foo" {:column-names (keys attrs)}))
	    attrs)
	 {:foo {:bar "ok", "3" 5}}
	 {:hello "world", 1 [3 5]})))

(deftest adding-collection
  (testing "Add collection of attributes into the database"
    (are [coll]
	 (= (do
	      (add-collection! table "foo" coll)
	      (vals (get-collection table "foo")))
	    coll)
	 [{:hello "world", :ok ":-)"}
	  {:hello "moon", :ok ":-("}])))
