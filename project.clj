(defproject tergueri "0.0.1-SNAPSHOT"
  :description "Very incomplete wrapper for Amazon's AWS SDK for Java"
  :dependencies [[org.clojure/clojure "1.2.0-beta1"]
                 [org.clojure/clojure-contrib "1.2.0-beta1"]
		 [net.sf.saxon/saxon9he "9.0.2j"]
		 [com.amazonaws/aws-java-sdk "1.0.005"]]
  :dev-dependencies [[swank-clojure "1.2.1"]]
  :repositories {"maven2-repository.dev.java.net" 
		 "http://download.java.net/maven/2/"})
