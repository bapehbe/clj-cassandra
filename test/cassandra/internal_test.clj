(ns cassandra.internal-test
  (:use [clojure.test]
	[cassandra.internal :reload-all true])
  (:import [org.apache.cassandra.thrift IndexOperator]))

(deftest encode-decode
  (testing "Encode and decode must be companions."
    (are [data] (= (decode (bytes-decode (encode data))) data)
	 {:foo "bar", 1 3, 1.25 :ok}
	 #{"hello", nil, "world", :haha})))

(deftest test-translate-operator 
  (is (= IndexOperator/EQ  (translate-operator '=)))
  (is (= IndexOperator/GTE (translate-operator '>=)))
  (is (= IndexOperator/GT  (translate-operator '>)))
  (is (= IndexOperator/LTE (translate-operator '<=)))
  (is (= IndexOperator/LT  (translate-operator '<))))
