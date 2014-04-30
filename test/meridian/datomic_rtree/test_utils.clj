(ns meridian.datomic-rtree.test-utils
  (:use [datomic.api :only (q db) :as d])
  (:require [clojure.java.io :as io]
            [meridian.datomic-rtree.rtree :as rtree]
            [meridian.datomic-rtree.bbox :as bbox]
            [meridian.datomic-rtree.hilbert :as hilbert]
            [meridian.datomic-rtree.bulk :as bulk]
            [meridian.datomic-rtree.shapes :as shapes]))

(defn find-tree [db]
  (->> (d/q '[:find ?e :where [?e :rtree/root]] db)
       ffirst (d/entity db)))

(defn create-and-connect-db [uri & schemas]
  (d/delete-database uri)
  (d/create-database uri)
  (let [conn (d/connect uri)]
    (doseq [schema schemas]
      (->> (read-string (slurp schema))
          (d/transact conn)))
    conn))

(defn rand-entries []
  (let [hilbert-index (hilbert/index-fn 28 [0.0 600.0])]
    (->> (repeatedly #(bbox/bbox (rand 540) (rand 540) (+ 10 (rand 50)) (+ 10 (rand 50))))
         (map (fn [box]
                (let [{max-x :bbox/max-x min-x :bbox/min-x
                       max-y :bbox/max-y min-y :bbox/min-y} box]
                  (shapes/create-entry
                   {:type :Feature
                    :bbox [min-x min-y max-x max-y]
                    :geometry {:type :Polygon
                               :coordinates [[[min-x max-x] [max-x min-y]
                                              [max-x max-y] [min-x max-y] [min-x max-x]]]}}
                   hilbert-index)))))))

(defn install-rand-data [conn num-entries]
  (let [test-data (take num-entries (rand-entries))]
    (time (doseq [entry test-data]
            @(d/transact conn [[:rtree/insert-entry (find-tree (d/db conn)) entry]])))
    :done))

(defn install-rand-ents [conn num-entries]
  (let [test-data (mapv #(assoc % :db/id (d/tempid :db.part/user))
                        (take num-entries (rand-entries)))]
    (time @(d/transact conn test-data))
    :done))

(defn create-tree-and-install-rand-data [conn num-entries max-children min-children]
  @(d/transact conn (rtree/create-tree-tx max-children min-children))
  (install-rand-data conn num-entries))

(defn install-and-bulk-load [conn num-entries max-children min-children]
  (install-rand-ents conn num-entries)
  (let [ents (rtree/hilbert-ents (d/db conn))]
    @(d/transact conn
                 (bulk/bulk-tx ents #db/id[:db.part/user]
                               max-children min-children bbox/area))
    :done))
