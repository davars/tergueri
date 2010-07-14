(ns vidston-aws.s3
  (:use vidston-.common)
  (:import java.io.ByteArrayInputStream
	   com.amazonaws.services.s3.AmazonS3Client
	   (com.amazonaws.services.s3.model ObjectMetadata)))
   
(defn s3-client []
  (AmazonS3Client. (credentials)))

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

(defn in-bucket? [bucket-name key]
  (try
   (.getObjectMetadata (s3-client) bucket-name key)
   true
   (catch Exception e false)))

