(ns vidston-aws.simpledb
  (:require [clojure.contrib.string :as str]
	    [vidston-aws.common :as common])
  (:import com.amazonaws.services.simpledb.AmazonSimpleDBClient
	   com.amazonaws.services.simpledb.util.SimpleDBUtils
	   (com.amazonaws.services.simpledb.model Attribute
						  BatchPutAttributesRequest
						  CreateDomainRequest
						  DeleteAttributesRequest
						  DeleteDomainRequest
						  GetAttributesRequest
						  Item
						  PutAttributesRequest
						  ReplaceableAttribute
						  ReplaceableItem
						  SelectRequest)))

(def ^{:doc "If bound, all functions will communicate through this
             client object.  Otherwise, a new client will be created
             for each call."}  
     *sdb-client* nil)

(defn sdb-client 
  "Gets an SimpleDB client object by either returning the one bound to
  *sdb-client* or creating a new one."  
  []
  (or *sdb-client*
      (AmazonSimpleDBClient. (common/credentials))))

(defn create-domain 
  "Creates the domain if it doesn't exist."
  [domain]
  (.createDomain (sdb-client) (CreateDomainRequest. domain)))

(defn delete-domain 
  "Deletes the domain if it exists."
  [domain]
  (.deleteDomain (sdb-client) (DeleteDomainRequest. domain)))

(defn- attribute [] (Attribute.))
(defn- replaceable-attribute [] (ReplaceableAttribute.))
(defn- create-attribute [attribute-factory key value]
  (doto (attribute-factory)
    (.setName (name key))
    (.setValue (common/encode-value value))))

(defn- attribute-collection [attribute-factory item-map]
  (let [item-map (dissoc item-map :id)
	attributes (for [[k v] item-map]
		     (if (vector? v)
		       (map #(create-attribute attribute-factory k %) v)
		       (create-attribute attribute-factory k v)))]
    (flatten attributes)))

(defn- replaceable-item [{id :id :as item-map}]
  (let [attributes (attribute-collection replaceable-attribute
					 item-map)]
    (doto (ReplaceableItem.)
      (.setName id)
      (.setAttributes attributes))))

(defn- attrs->map [attrs]
  (let [attrs (for [attr attrs]		  
		{(keyword (.getName attr))
		 (common/decode-value (.getValue attr))})]
    (apply merge-with (comp vec flatten vector) attrs)))  

(defn- item->map [item]
  (let [attr-map (attrs->map (.getAttributes item))]
    (assoc attr-map :id (.getName item))))

(defn delete-attributes 
  "Deletes all matching attributes in the given item-map from the domain."
  [domain {id :id :as item-map}]
  (let [client (sdb-client)
	attributes (attribute-collection attribute 
					 item-map)]
    (.deleteAttributes client (doto (DeleteAttributesRequest. domain id)
				(.setAttributes attributes)))))

(defn put-attributes 
  "Puts the attributes in item-map into the domain, either
  replacing existing attributes or adding to them."  
  [domain {id :id :as item-map} replace?]
  (let [client (sdb-client)
	attributes (attribute-collection replaceable-attribute
					 item-map)
	attributes (if replace? 
		     (map #(doto % (.setReplace true)) attributes)
		     attributes)]
  (.putAttributes client (PutAttributesRequest. domain id attributes))))

(defn batch-put-attributes 
  "Puts the collections of item-maps into the domain, either
  replacing existing attributes or adding to them."
  [domain item-maps replace?]
  (let [client (sdb-client)
	items (map replaceable-item item-maps)
	set-replace (fn [item]
		      (doseq [attr (.getAttributes item)]
			(.setReplace attr true)))]
    (if replace?
      (doseq [item items]
	(set-replace item)))
    (.batchPutAttributes 
     client
     (BatchPutAttributesRequest. domain items))))

(defn get-attributes 
  "Gets item-name as an item-map."
  [domain item-name]
  (let [client (sdb-client)
	attributes (.getAttributes 
		    (.getAttributes 
		     client
		     (GetAttributesRequest. domain item-name)))
	item (doto (Item.)
	       (.setAttributes attributes)
	       (.setName item-name))]
    (item->map item)))

(defn- select [client q next-token]
  (.select client (doto (SelectRequest. q)
		    (.setNextToken next-token))))
  
(defn- run-query
  ([client q]
     (run-query client q nil))
  ([client q next-token]
     (let [result (select client q next-token)
	   next-token (.getNextToken result)
	   results (map item->map (.getItems result))]
       (if next-token
	 (lazy-cat results
		   (run-query client q next-token))
	 results))))

(defn- order-string [[attribute direction]]
  (str " order by " (name attribute) " " (name direction)))
(defn- where-string [where-clause]
  (str " where " where-clause))
(defn- query-string [{:keys [domain attributes where sort limit]}]
  (str "select " (str/join "," (or attributes ["*"]))
       " from `" domain "`"
       (if where (where-string where))
       (if sort (order-string sort))))

(defn- count-string [{:keys [offset] :as query-map}]
  (-> query-map
      (assoc :attributes ["count(*)"])
      query-string
      (str " limit " offset)))

(defn- skip-token 
  [client {:keys [offset] :as query-map}]
  (let [q (count-string query-map)
	result (select client q (:next-token query-map))
	n (-> result .getItems first item->map :Count)
	new-next-token (.getNextToken result)]
    (if (or (not new-next-token) (= n offset))
      new-next-token
      (recur client
	     (merge query-map {:offset (- offset n)
			       :next-token new-next-token})))))

(defn query
  [{:keys 
    [domain attributes where sort limit offset next-token] 
    :as query-map}]
  (let [client (sdb-client)
	token (if (and offset (> offset 0))
		(skip-token client query-map))
	results (run-query client (query-string query-map) token)]
    (if limit
      (take limit results)
      results)))
	
(defn count-items [domain where]
  "Returns the number of items in domain matching the SimpleDB query 'where' clause."
  (-> (sdb-client)
      (query {:domain domain :attributes ["count(*)"] :where where})
      first
      :Count))