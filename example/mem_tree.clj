(ns mem-tree
  (:use [datomic.api :only (q db) :as d])
  (:require [clojure.java.io :as io]
            [meridian.datomic-rtree.rtree :as rtree]
            [meridian.datomic-rtree.bbox :as bbox])
  (:import datomic.Util))

(def uri "datomic:mem://rtrees")

(defn find-tree [db]
  (->> (d/q '[:find ?e :where [?e :rtree/root]] db)
       ffirst (d/entity db)))

(defn create-and-connect-db
  ([uri schema] (create-and-connect-db uri schema 50 20))
  ([uri schema max-children min-children]
     (d/delete-database uri)
     (d/create-database uri)
     (let [conn (d/connect uri)]
       (->> (read-string (slurp schema))
            (d/transact conn))
       (d/transact conn (rtree/create-tree-tx max-children min-children))
       conn)))

(defn install-test-data [conn]
  (let [test-data [[[0.0 0.0 10.0 10.0] "a"]
                   [[5.0 5.0 8.0 8.0] "b"]
                   [[0.0 0.0 20.0 20.0] "c"]
                   [[2.0 2.0 6.0 6.0] "d"]
                   [[-10.0 -5.0 5.0 5.0] "e"]
                   [[-100.0 -50.0 5.0 5.0] "f"]
                   [[-1.0 -5.0 50.0 50.0] "g"]
                   [[1.0 1.0 8.0 8.0] "h"]]]
    (doseq [[box entry] test-data]
      (d/transact conn [[:rtree/insert-entry
                         (find-tree (d/db conn))
                         (assoc (apply bbox/extents box) :node/entry entry)]]))))

(defn install-rand-data [conn num-entries]
  (let [test-data (into [] (take num-entries (partition 4 (repeatedly #(rand 500)))))]
    (time (doseq [box test-data]
            (d/transact conn [[:rtree/insert-entry
                               (find-tree (d/db conn))
                               (assoc (apply bbox/extents box)
                                 :node/entry (str (char (rand-int 200))))]])))))

(defn print-tree [conn]
  (let [root (:rtree/root (find-tree (d/db conn)))]
    ((fn walk [n indent]
       (println (str indent (:db/id n) " " (:node/entry n)))
       (doseq [c (:node/children n)]
         (walk c (str indent "---"))))
     root "")))

(comment
  (def conn (create-and-connect-db uri "resources/datomic/schema.edn" 6 3))
  (install-rand-data conn 1000)
  (time (count (d/q '[:find ?e
                      :in $
                      :where
                      [?e :node/entry]
                      [(datomic.api/entity $ ?e) ?b]
                      [(meridian.datomic-rtree.bbox/intersects?
                        (meridian.datomic-rtree.bbox/extents 0.0 0.0 100.0 100.0) ?b)]] (d/db conn))))
  (time (count (rtree/intersecting (:rtree/root (find-tree (d/db conn)))
                                   (bbox/extents 0.0 0.0 100.0 100.0)))))
