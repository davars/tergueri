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
