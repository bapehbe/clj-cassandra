(defproject clj-cassandra "0.6.0"
  :description "Clojure client for Apache Cassandra"
  :source-path "src/clj"
  :java-source-path "src/java"
  :javac-fork "true"
  :dependencies [[org.clojure/clojure-contrib "1.1.0"]
		 [org.apache.cassandra/cassandra "0.6.0-rc1"]
		 [org.apache.thrift/thrift "r917130"]
		 [com.eaio.uuid/uuid "3.1"]
		 [org.slf4j/slf4j-api "1.5.8"]]
  :dev-dependencies [[leiningen/lein-swank "1.1.0"]
		     [org.clojars.mmcgrana/lein-clojars "0.5.0"]
		     [org.clojars.mmcgrana/lein-javac "0.1.0"]
		     [org.slf4j/slf4j-log4j12 "1.5.8"]
		     [log4j "1.2.14"]])