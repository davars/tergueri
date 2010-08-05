(ns tergueri.util
  (:require [clojure.contrib.json :as json])
  (:use (tergueri sqs sns)))

(defn bind [queue-name topic-name]
  (let [queue-url (create-queue queue-name)
	queue-arn ((get-queue-attributes queue-url 
					 ["QueueArn"]) 
		   "QueueArn")
	topic-arn (create-topic topic-name)
	policy-id (.toString (java.util.UUID/randomUUID))
	policy {:Version "2008-10-17"
		:Id policy-id
		:Statement
		[{:Sid policy-id
		  :Effect "Allow",
		  :Principal {:AWS "*"},
		  :Action "SQS:SendMessage",
		  :Resource queue-arn
		  :Condition
		  {:StringLike {:AWS:SourceArn topic-arn}}}]}]
    (set-queue-attributes queue-url {"Policy" (json/json-str policy)})
    (subscribe topic-arn "sqs" queue-arn)))
