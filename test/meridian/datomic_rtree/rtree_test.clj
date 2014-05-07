(ns meridian.datomic-rtree.rtree-test
  (:require [datomic.api :as d]
            [clojure.test :refer :all]
            [meridian.datomic-rtree.rtree :refer :all]
            [meridian.datomic-rtree.bbox :as bbox]
            [meridian.datomic-rtree.test-utils :as utils]))

(deftest bbox-area-test
  (are [bbox area] (== (bbox/area bbox) area)
       {:bbox/min-x 0 :bbox/min-y 0
        :bbox/max-x 10 :bbox/max-y 10} 100
       {:bbox/min-x -10 :bbox/min-y -10
        :bbox/max-x 10 :bbox/max-y 10} 400))

(deftest bbox-union-test
  (is (= (bbox/union {:bbox/min-x 0 :bbox/min-y 0
                       :bbox/max-x 10 :bbox/max-y 10}
                      {:bbox/min-x -10 :bbox/min-y -10
                       :bbox/max-x 20 :bbox/max-y 5})
          {:bbox/min-x -10 :bbox/min-y -10
           :bbox/max-x 20 :bbox/max-y 10})))

(deftest choose-leaf-test
  (let [child1 {:bbox/min-x 0 :bbox/min-y 170
                :bbox/max-x 10 :bbox/max-y 10
                :node/is-leaf? true}
        child2 {:bbox/min-x 10 :bbox/min-y 0
                :bbox/max-x 200 :bbox/max-y 200
                :node/is-leaf? true}
        child3 {:bbox/min-x 50 :bbox/min-y 50
                :bbox/max-x 60 :bbox/max-y 60
                :node/is-leaf? true}
        new-bbox {:bbox/min-x 0 :bbox/min-y 0
                  :bbox/max-x 20 :bbox/max-y 20}
        root {:node/children #{child1 child2 child3}}]
    (is (== (bbox/enlargement child1 new-bbox)
            (bbox/enlargement child2 new-bbox)))
    (is (= (choose-leaf root new-bbox) child1))))

(deftest pick-seeds-test
  (let [n1 {:bbox/min-x 0 :bbox/min-y 0
            :bbox/max-x 10 :bbox/max-y 10}
        n2 {:bbox/min-x 10 :bbox/min-y 10
            :bbox/max-x 200 :bbox/max-y 200}
        n3 {:bbox/min-x 50 :bbox/min-y 50
            :bbox/max-x 60 :bbox/max-y 60}
        n4 {:bbox/min-x 1000 :bbox/min-y 1000
            :bbox/max-x 1500 :bbox/max-y 1500}]
    (is (= (set (pick-seeds #{n1 n2 n3 n4})) #{n1 n4}))))

(deftest pick-next-test
  (let [n1 {:bbox/min-x 0 :bbox/min-y 0
            :bbox/max-x 10 :bbox/max-y 10}
        n2 {:bbox/min-x 10 :bbox/min-y 10
            :bbox/max-x 20 :bbox/max-y 20}
        n3 {:bbox/min-x 500 :bbox/min-y 500
            :bbox/max-x 600 :bbox/max-y 600}
        n4 {:bbox/min-x 550 :bbox/min-y 550
            :bbox/max-x 600 :bbox/max-y 600}
        n5 {:bbox/min-x 0 :bbox/min-y 0
            :bbox/max-x 20 :bbox/max-y 20}
        n6 {:bbox/min-x 20 :bbox/min-y 20
            :bbox/max-x 500 :bbox/max-y 500}]
    (is (= (pick-next #{n1 n2} #{n3 n4} #{n5 n6}) n5))))

(deftest split-node-test
  (let [n1 {:bbox/min-x 0 :bbox/min-y 0
            :bbox/max-x 10 :bbox/max-y 10}
        n2 {:bbox/min-x 10 :bbox/min-y 10
            :bbox/max-x 20 :bbox/max-y 20}
        n3 {:bbox/min-x 500 :bbox/min-y 500
            :bbox/max-x 600 :bbox/max-y 600}
        n4 {:bbox/min-x 550 :bbox/min-y 550
            :bbox/max-x 600 :bbox/max-y 600}
        n5 {:bbox/min-x 0 :bbox/min-y 0
            :bbox/max-x 20 :bbox/max-y 20}
        n6 {:bbox/min-x 20 :bbox/min-y 20
            :bbox/max-x 500 :bbox/max-y 500}]
    (is (= (split-node #{n1 n2 n3 n4 n5 n6} 2)
           [#{n3 n4} #{n1 n2 n5 n6}]))))

(defn all-entries [db]
  (mapv #(d/entity db (first %))
        (d/q '[:find ?e :where [?e :node/entry]] db)))

(deftest test-tree-creation
  (let [uri "datomic:mem://rtrees"
        num-entries 100
        conn (utils/create-and-connect-db uri
                                          "resources/datomic/schema.edn"
                                          "resources/datomic/geojsonschema.edn")]
    (utils/create-tree conn 6 3)
    (utils/install-rand-ents conn num-entries utils/create-feature)
    (utils/load-ents conn 6 3)
    (is (= num-entries (count (all-entries (d/db conn)))))))

(deftest test-bulk-tree-creation
  (let [uri "datomic:mem://rtrees"
        num-entries 100
        conn (utils/create-and-connect-db uri
                                          "resources/datomic/schema.edn"
                                          "resources/datomic/geojsonschema.edn")]
    (utils/install-rand-ents conn num-entries utils/create-feature)
    (utils/bulk-load-ents conn 6 3)
    (is (= num-entries (count (all-entries (d/db conn)))))))
