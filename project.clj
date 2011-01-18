(defproject clj-cassandra "0.1.3"
  :description "Clojure client for Apache Cassandra"
  :source-path "src/clj"
  :java-source-path "src/java"
  :javac-fork "true"
  :omit-default-repositories false
  :repositories {"eaio.com" "http://eaio.com/maven2"}
  :dependencies [[org.clojure/clojure-contrib "1.2.0"]
		 [org.apache.cassandra/cassandra-all "0.7.0"]
		 [com.eaio.uuid/uuid "3.1"]
		 [org.slf4j/slf4j-log4j12 "1.5.8"]
		 [log4j "1.2.14"]
		 [org.slf4j/slf4j-api "1.5.8"]]
  :dev-dependencies [[swank-clojure "1.2.1"]
		     [lein-clojars "0.5.0"]
		     [org.clojars.mmcgrana/lein-javac "1.2.1"]])
