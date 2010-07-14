(ns vidston-aws.sns
  (:use (clojure.contrib json)
	vidston-aws.common)
  (:import com.amazonaws.services.sns.AmazonSNSClient
	   (com.amazonaws.services.sns.model CreateTopicRequest
					     DeleteTopicRequest
					     GetTopicAttributesRequest
					     PublishRequest
					     SubscribeRequest)))

(defn sns-client []
  (AmazonSNSClient. (credentials)))

(defn create-topic [client topic-name]
  (.getTopicArn
   (.createTopic client (CreateTopicRequest. topic-name))))

(defn delete-topic [client arn]
  (.deleteTopic client (DeleteTopicRequest. arn)))

(defn get-topic-attributes [client arn]
  (let [result (.getTopicAttributes client (GetTopicAttributesRequest. arn))
	attr-map (into {} (.getAttributes result))]
    (update-in attr-map ["Policy"] read-json)))

(defn subscribe [client arn proto endpoint]
  (.subscribe client (SubscribeRequest. 
		      arn proto endpoint)))

(defn publish
  "Publishes clojure value as a print-dup'd string"
  [client arn message]
  (.publish client (PublishRequest. arn (encode-value message))))
