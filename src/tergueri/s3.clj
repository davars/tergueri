(ns tergueri.s3
  (:use tergueri.common)
  (:import java.io.ByteArrayInputStream
	   com.amazonaws.services.s3.AmazonS3Client
	   (com.amazonaws.services.s3.model ObjectMetadata)))

(def ^{:doc 
  "If bound, all functions will communicate through this client
  object. Otherwise, a new client will be created for each call."}
  *s3-client* nil)

(defn s3-client 
  "Gets a SimpleDB client object by either returning the one bound to
  *sdb-client* or creating a new one."  
  []
  (or *s3-client*
      (AmazonS3Client. (credentials))))

(defn put-bytes
  [bucket-name key content-type bytes]
  (let [bais (ByteArrayInputStream. bytes)
	metadata (doto (ObjectMetadata.)
		   (.setContentLength (.available bais))
		   (.setContentType content-type))]
    (.putObject (s3-client) bucket-name key bais metadata)))

(defn get-object-content
  [bucket-name key]
  (let [obj (.getObject (s3-client) bucket-name key)]
    (.getObjectContent obj)))

(defn get-bytes
  [bucket-name key]
  (is->bytes (get-object-content bucket-name key)))
	
(defn copy-object [src-bucket-name src-key
		   dst-bucket-name dst-key]
  (.copyObject (s3-client) 
	       src-bucket-name src-key
	       dst-bucket-name dst-key))

(defn time-in [millis]
  (java.util.Date. (+ (.getTime (java.util.Date.)) millis)))

(defn create-url [bucket-name key valid-for-millis]
  (.generatePresignedUrl (s3-client) bucket-name key (time-in valid-for-millis)))

(defn object-metadata [bucket-name key]
  (try
   (.getObjectMetadata (s3-client) bucket-name key)
   (catch com.amazonaws.services.s3.model.AmazonS3Exception e nil)))

(defn in-bucket? [bucket-name key]
  (if (object-metadata bucket-name key) true false))

(defn- object-summary-seq [listing]
  (lazy-cat
   (.getObjectSummaries listing)
   (if (.isTruncated listing) 
     (object-summary-seq
      (.listNextBatchOfObjects (s3-client) listing)))))

(defn list-objects [bucket]
  (let [listing (-> (s3-client)
		    (.listObjects bucket)
		    object-summary-seq)]
    (map bean listing)))
