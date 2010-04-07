(ns cassandra.client-test
  "Tests which need a cassandra setup. Using the immediate
   Cassandra downloaded configuration to test."
  (:use [cassandra.client :reload-all true]
	[clojure.test]))

(def table (-> (mk-client "localhost" 9160)
	       (key-space "Keyspace1")
	       (column-family "Standard1")))

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
	      (get-attrs table "foo" (keys attrs))) attrs)
	 {:hello "world", 1 [3 5]})))
	 