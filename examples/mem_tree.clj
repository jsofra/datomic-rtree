(ns mem-tree
  (:use [datomic.api :only (q db) :as d])
  (:require [clojure.java.io :as io]
            [meridian.datomic-rtree.rtree :as rtree]
            [meridian.datomic-rtree.bbox :as bbox]
            [meridian.datomic-rtree.hilbert :as hilbert]
            [meridian.datomic-rtree.bulk :as bulk])
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
       @(d/transact conn (rtree/create-tree-tx max-children min-children))
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
      @(d/transact conn [[:rtree/insert-entry
                          (find-tree (d/db conn))
                          (assoc (apply bbox/extents box) :node/entry entry)]]))))

(defn install-rand-data [conn num-entries]
  (let [hilbert-index (hilbert/index-fn 28 [0.0 500.0])
        test-data (into [] (take num-entries (partition 4 (repeatedly #(rand 500)))))]
    (time (doseq [[x1 y1 x2 y2 :as box] test-data]
            @(d/transact conn [[:rtree/insert-entry
                                (find-tree (d/db conn))
                                (merge (apply bbox/extents box)
                                       {:node/entry (str (char (rand-int 200)))})]])))))


(defn install-rand-ents [conn num-entries]
  (let [hilbert-index (hilbert/index-fn 28 [0.0 500.0])
        test-data (->> (take num-entries (partition 4 (repeatedly #(rand 500))))
                       (map (fn [[x1 y1 x2 y2 :as box]]
                              (merge (apply bbox/extents box)
                                     {:node/entry (str (char (rand-int 200)))
                                      :db/id (d/tempid :db.part/user)
                                      :node/hilbert-val (hilbert-index
                                                         [(* 0.5 (+ x1 x2))
                                                          (* 0.5 (+ y1 y2))])}))))]
    (time @(d/transact conn test-data))
    :done))

(defn print-tree [conn]
  (let [root (:rtree/root (find-tree (d/db conn)))]
    ((fn walk [n indent]
       (println (str indent (:db/id n) " " (:node/entry n)))
       (doseq [c (:node/children n)]
         (walk c (str indent "---"))))
     root "")))

(defn all-entries [db]
  (map #(d/entity db (first %))
       (d/q '[:find ?e :where [?e :node/entry]] db)))

(defn naive-intersecting [entries search-box]
  (into [] (filter #(bbox/intersects? search-box %) entries)))

(defn ent-intersects? [box ent db]
  (bbox/intersects? box (d/entity db ent)))

(def search-rules
  '[[(intersecting ?ancestor ?descendant ?search-box)
     [(mem-tree/ent-intersects? ?search-box ?descendant $)]
     [?ancestor :node/children ?descendant]]
    [(intersecting ?ancestor ?descendant ?search-box)
     [?ancestor :node/children ?child]
     [intersecting ?child ?descendant ?search-box]]])

(def intersecting-q
  '[:find ?e
    :in $ % ?root ?bbox
    :where
    [intersecting ?root ?e ?bbox]
    [?e :node/entry]])

(defn install-and-bulk-load [conn n]
  (install-rand-ents conn n)
  (let [ents (rtree/hilbert-ents (d/db conn))]
    @(d/transact conn
                 (bulk/bulk-tx ents #db/id[:db.part/user] 6 3 bbox/area))
    :done))

(comment
  (def conn (create-and-connect-db uri "resources/datomic/schema.edn" 6 3))
  (install-rand-data conn 1000)
  (install-rand-ents conn 100)
  (def search-box (bbox/extents 0.0 0.0 10.0 10.0))
  (def root (:rtree/root (find-tree (d/db conn))))
  (time (count (naive-intersecting (all-entries (d/db conn)) search-box)))
  (time (count (rtree/intersecting root search-box)))
  (time (count (d/q intersecting-q (d/db conn) search-rules (:db/id root) search-box)))
  (time (install-and-bulk-load conn 10000))
  )
