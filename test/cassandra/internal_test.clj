(ns cassandra.internal-test
  (:use [clojure.test]
	[cassandra.internal :reload-all true])
  (:import [org.apache.cassandra.thrift IndexOperator IndexExpression]))

(deftest encode-decode
  (testing "Encode and decode must be companions."
    (are [data] (= (decode (bytes-decode (encode data))) data)
	 {:foo "bar", 1 3, 1.25 :ok}
	 #{"hello", nil, "world", :haha})))

(deftest test-operator 
  (is (= IndexOperator/EQ  (operator '=)))
  (is (= IndexOperator/GTE (operator '>=)))
  (is (= IndexOperator/GT  (operator '>)))
  (is (= IndexOperator/LTE (operator '<=)))
  (is (= IndexOperator/LT  (operator '<))))

(deftest test-expression
  (is (= (IndexExpression. (encode :bar) (operator '>=) (encode :foo))
         (expression '(>= :bar :foo)))))
