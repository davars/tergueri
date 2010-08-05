(ns tergueri.common
  (:use clojure.java.io
	clojure.contrib.str-utils
	tergueri.settings)
  (:import com.amazonaws.services.simpledb.util.SimpleDBUtils))

(def ^{:doc 
  "If bound, will be returned by all calls to credentials.  Otherwise,
  a new credentials object will be created using
  settings/get-setting."} 
  *credentials* nil)

(defn credentials []
  (or *credentials*
      (com.amazonaws.auth.BasicAWSCredentials.
       (get-setting "aws.key.access")
       (get-setting "aws.key.secret"))))

(defmulti encode-value
  "Encodes a value for storage in a SimpleDB attribute"
  type)

(defn decode-date [date]
  (SimpleDBUtils/decodeDate date))

(defmethod encode-value java.util.Date [date]
  (str "#=(decode-date \""
       (SimpleDBUtils/encodeDate date)
       \"")"))

(defmethod encode-value :default [value]
    (pr-str value))

(defn decode-value [s]
  (binding [*ns* (the-ns ' tergueri.common)]
    (read-string s)))

(defn is->bytes [is]
  (with-open [os (java.io.ByteArrayOutputStream.)]
    (copy is os)
    (.toByteArray os)))
