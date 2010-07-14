(ns vidston-aws.settings
  (:use clojure.contrib.properties)
  (:import java.util.ResourceBundle))

(defn resource-properties [bundle]
  (let [keys (enumeration-seq (.getKeys bundle))
	entries (map #(vector % (.getString bundle %)) keys)]
    (as-properties entries)))

(let [props (if-let [prop-file (System/getProperty "aws.properties.file")]
	      (read-properties prop-file)
	      (resource-properties (ResourceBundle/getBundle "aws")))]
  (set-system-properties props))

(defn get-setting [key]
  (if-let [prop (System/getProperty key)]
    prop
    (throw (Exception. 
	    (format "Required property %s hasn't been set" key)))))

