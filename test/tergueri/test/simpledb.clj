(ns tergueri.test.simpledb
  (:use [tergueri.simpledb] :reload-all)
  (:use [clojure.test]))

(deftest test-where
  (testing "Where expressions"
    (are [expr output] (= expr output)
	 (translate-expr (> Foo-bar_baz (+ 1 4)))
	 "`Foo-bar_baz` > '5'"
	 (translate-expr (and (< Foo "bar") (= Bar :five))) 
	 "(`Foo` < '\"bar\"') and (`Bar` = ':five')"
	 (translate-expr (and (< (every Foo) "bar") (= Bar :five)))
	 "(every(`Foo`) < '\"bar\"') and (`Bar` = ':five')")))

(deftest demo
  (create-domain "test-domain")
  (put-attributes "test-domain" {:id "id1" 
			       :key1 "val1-1" 
			       :key2 {:some "map" :of :values}} true)
  (put-attributes "test-domain" {:id "id2" 
			       :key1 "val1-2" 
			       :key3 {:some "map" :of :values}} true)
  (are [expr output] (= expr output)
       (select test-domain) [{:id "id1", 
			      :key1 "val1-1", 
			      :key2 {:some "map", :of :values}} 
			     {:id "id2", 
			      :key3 {:some "map", :of :values}, 
			      :key1 "val1-2"}]
       (select test-domain [count])        [{:id "Domain", :Count 2}]
       (select test-domain [itemName])     [{:id "id1"} {:id "id2"}]
       (select test-domain [key1])         [{:id "id1", :key1 "val1-1"} 
				            {:id "id2", :key1 "val1-2"}]
       (select test-domain 
       	       (where (is-not-null key2))) [{:id "id1", 
       					     :key1 "val1-1", 
       					     :key2 {:some "map", :of :values}}]))