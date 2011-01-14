(ns cassandra.internal-test
  (:use [clojure.test]
	[cassandra.internal :reload-all true]))

(deftest encode-decode
  (testing "Encode and decode must be companions."
    (are [data] (= (decode (bytes-decode (encode data))) data)
	 {:foo "bar", 1 3, 1.25 :ok}
	 #{"hello", nil, "world", :haha})))
