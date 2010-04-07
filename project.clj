(defproject clj-cassandra "0.6.0"
  :description "Clojure client for Apache Cassandra"
  :dependencies [[org.clojure/clojure-contrib "1.1.0"]
		 [org.apache.cassandra/cassandra "0.6.0-beta2"]
		 [org.apache.thrift/thrift "r894924"]
		 [org.slf4j/slf4j-api "1.5.8"]]
  :dev-dependencies [[leiningen/lein-swank "1.1.0"]
		     [org.slf4j/slf4j-log4j12 "1.5.8"]
		     [log4j "1.2.14"]])