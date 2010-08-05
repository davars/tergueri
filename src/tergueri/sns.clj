(ns tergueri.sns
  (:require [tergueri.common :as common]
	    [clojure.contrib.json :as json])
  (:import com.amazonaws.services.sns.AmazonSNSClient
	   (com.amazonaws.services.sns.model CreateTopicRequest
					     DeleteTopicRequest
					     GetTopicAttributesRequest
					     PublishRequest
					     SubscribeRequest)))

(def ^{:doc 
  "If bound, all functions will communicate through this client object.
  Otherwise, a new client will be created for each call."}
  *sns-client* nil)

(defn sns-client 
  "Gets an SNS client object by either returning the one bound to
  *sdb-client* or creating a new one."  
  []
  (or *sns-client*
      (AmazonSNSClient. (common/credentials))))

(defn create-topic [topic-name]
  (.getTopicArn
   (.createTopic (sns-client) (CreateTopicRequest. topic-name))))

(defn delete-topic [arn]
  (.deleteTopic (sns-client) (DeleteTopicRequest. arn)))

(defn get-topic-attributes [arn]
  (let [result (.getTopicAttributes (sns-client) (GetTopicAttributesRequest. arn))
	attr-map (into {} (.getAttributes result))]
    (update-in attr-map ["Policy"] json/read-json)))

(defn subscribe [arn proto endpoint]
  (.subscribe (sns-client) (SubscribeRequest. 
			    arn proto endpoint)))

(defn publish
  "Publishes clojure value as a print-dup'd string"
  [arn message]
  (.publish (sns-client) (PublishRequest. arn (common/encode-value message))))
