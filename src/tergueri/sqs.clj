(ns
    #^{:author "Dave Jack"
       :doc "This is a highly simplified interface to SQS.  For best
       results, use send-message to send any clojure value (except a
       symbol, as we'll see later), which will be serialized and sent
       down the wire.  receive-messages spits out maps that can be
       passed direclty to delete-message.  These maps have a :body
       value, which has been conveniently deserialized.

       SNS creates a wrinkle by dumping a base64-encoded JSON string
       where SQS would dump a plain old string.  Fortunately, since
       it's unlikely that you'd ever want to send a plain symbol as
       your message, we decide to disallow this and treat plain
       symbols as SNS notifications, which get base64 and JSON
       decoded.  The :Message field of the JSON object then becomes
       the :body of the message map returned.

       If you're using this lib to receive messages but want send them
       using something else, send \"strings\" instead of strings."}

  tergueri.sqs
  (:require [tergueri.common :as common]
	    [clojure.contrib.json :as json])
  (:import com.amazonaws.services.sqs.AmazonSQSClient
	   (com.amazonaws.services.sqs.model CreateQueueRequest
					     DeleteQueueRequest
					     DeleteMessageRequest
					     GetQueueAttributesRequest
					     ReceiveMessageRequest
					     SendMessageRequest
					     SetQueueAttributesRequest)
	   org.apache.commons.codec.binary.Base64))
(def ^{:doc 
  "If bound, all functions will communicate through this client object.
  Otherwise, a new client will be created for each call."}
  *sqs-client* nil)

(defn sqs-client []
  (or *sqs-client*
      (AmazonSQSClient. (common/credentials))))

(defn create-queue [queue-name]
  (.getQueueUrl (.createQueue (sqs-client) 
			      (CreateQueueRequest. queue-name))))

(defn delete-queue [queue-url]
  (.deleteQueue (sqs-client) (DeleteQueueRequest. queue-url)))

(defn send-message [url body]
  {:pre [(not (= clojure.lang.Symbol (type body)))]}
  (.sendMessage (sqs-client) (SendMessageRequest. 
			      url
			      (common/encode-value body))))

(defn- decode-base64 [message]
  (String. (Base64/decodeBase64 (.getBytes message))))

(defn- unwrap-sns [body] 
  (if (= clojure.lang.Symbol (type body))
    (-> body 
	str 
	decode-base64
	json/read-json
	(:Message)
	common/decode-value)
    body))

(defn- read-body [body] 
  (-> body 
      common/decode-value
      unwrap-sns))

(defn receive-messages 
  ([url vis-timeout]
     (receive-messages url 1 vis-timeout))
  ([url n vis-timeout]
     (let [request (doto (ReceiveMessageRequest. 
			  url)
		     (.setVisibilityTimeout vis-timeout)
		     (.setMaxNumberOfMessages n))
	   messages (.getMessages (.receiveMessage (sqs-client) request))]
       (seq (and messages 
		 (map #(-> %
			   bean
			   (assoc :queueUrl url)
			   (update-in [:body] read-body))
		      messages))))))

(defn delete-message [{:keys [queueUrl receiptHandle]}]
  (.deleteMessage (sqs-client)
		  (DeleteMessageRequest. queueUrl receiptHandle)))

(defn get-queue-attributes 
  ([url]
     (get-queue-attributes url ["All"]))
  ([url attribute-names]
     (let [request (doto (GetQueueAttributesRequest. url)
		     (.setAttributeNames attribute-names))
	   result (.getQueueAttributes (sqs-client) request)]
       (into {} (.getAttributes result)))))

(defn set-queue-attributes
  [url attributes]
  (.setQueueAttributes (sqs-client)
		       (SetQueueAttributesRequest.
			url attributes)))
