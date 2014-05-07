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

(defn create-feature [hilbert-index box]
  (let [{max-x :bbox/max-x min-x :bbox/min-x
         max-y :bbox/max-y min-y :bbox/min-y} box]
    (shapes/create-entry
     {:type :Feature
      :bbox [min-x min-y max-x max-y]
      :geometry {:type :Polygon
                 :coordinates [[[min-x max-x] [max-x min-y]
                                [max-x max-y] [min-x max-y] [min-x max-x]]]}}
     hilbert-index)))

(defn minimal-entry [hilbert-index box]
  (let [{max-x :bbox/max-x min-x :bbox/min-x
         max-y :bbox/max-y min-y :bbox/min-y} box]
    (shapes/create-entry
     {:type :Point
      :bbox [min-x min-y max-x max-y]}
     hilbert-index)))

(defn rand-entries [entry-constructor]
  (let [hilbert-index (hilbert/index-fn 28 [0.0 600.0])]
    (->> (repeatedly #(bbox/bbox (rand 540) (rand 540) (+ 10 (rand 50)) (+ 10 (rand 50))))
         (map (partial entry-constructor hilbert-index)))))

(defn install-rand-data [conn num-entries entry-constructor]
  (let [test-data (take num-entries (rand-entries entry-constructor))]
    (time (doseq [entry test-data]
            @(d/transact conn [[:rtree/insert-and-install-entry (find-tree (d/db conn)) entry]])))
    :done))

(defn install-rand-ents [conn num-entries entry-constructor]
  (let [test-data (take num-entries (rand-entries entry-constructor))]
    @(d/transact conn test-data)
    :done))

(defn p-install-rand-ents [conn num-entries entry-constructor]
  (let [test-data (take num-entries (rand-entries entry-constructor))
        chunks (partition-all (quot (count test-data)
                                    (* 8 (.. Runtime getRuntime availableProcessors)))
                              test-data)]
    (doall (pmap (fn [c] @(d/transact conn c)) chunks))
    :done))

(defn create-tree [conn max-children min-children]
  @(d/transact conn (rtree/create-tree-tx max-children min-children))
  :done)

(defn load-ents [conn]
  (let [ents (rtree/hilbert-ents (d/db conn))
        tree (find-tree (d/db conn))]
    (doseq [ent ents]
      @(d/transact conn [[:rtree/insert-entry tree ent]]))
    :done))

(defn bulk-load-ents [conn max-children min-children part-fn]
  (let [ents (rtree/hilbert-ents (d/db conn))]
    @(d/transact conn
                 (bulk/bulk-tx ents #db/id[:db.part/user]
                               max-children min-children bbox/area
                               part-fn))
    :done))

(defn all-entries [db]
  (map #(d/entity db (first %))
       (d/q '[:find ?e :where [?e :node/entry]] db)))

(defn naive-intersecting [entries search-box]
  (into [] (filter #(bbox/intersects? search-box %) entries)))

(defn install-timings [conn-fn num-entries max-children min-children]
  (let [time-installs (fn [conn install-fn entry-constructor heading]
                        (create-tree conn max-children min-children)
                        (println "------------------------------")
                        (println (str "Installing " num-entries " of " heading))
                        (time (install-fn conn num-entries entry-constructor)))]
    (println "------------------------------")
    (println "--# Entity Install Timings #--")
    (time-installs (conn-fn) install-rand-ents create-feature
                   "features with install-rand-ents.")
    (time-installs (conn-fn) p-install-rand-ents create-feature
                   "features with p-install-rand-ents.")))

(defn tree-build-timings [conn-fn num-entries max-children min-children]
  (let [time-builds (fn [conn build-fn heading]
                      (create-tree conn max-children min-children)
                      (p-install-rand-ents conn num-entries create-feature)
                      (println "------------------------------")
                      (println (str "Building tree with " num-entries " entities using " heading))
                      (time (build-fn conn)))]
    (println "--------------------------")
    (println "--# Tree Build Timings #--")
    (time-builds (conn-fn) load-ents "load-ents.")
    (time-builds (conn-fn) #(bulk-load-ents %1 max-children min-children bulk/cost-partition)
                 "bulk-load-ents and cost-partition.")
    (time-builds (conn-fn) #(bulk-load-ents %1 max-children min-children bulk/dyn-cost-partition)
                 "bulk-load-ents and dyn-cost-partition.")))

(defn search-timings [conn-fn num-entries max-children min-children]
  (let [build-tree (fn [conn]
                     (p-install-rand-ents conn num-entries minimal-entry)
                     (bulk-load-ents conn max-children min-children bulk/dyn-cost-partition))
        time-search (fn [conn search-fn search-box heading]
                      (println "------------------------------")
                      (println (str "Searching tree with " num-entries " entities using " heading))
                      (time (search-fn conn search-box)))
        naive-fn #(count (naive-intersecting (all-entries (d/db %1)) %2))
        rtree-fn #(count (rtree/intersecting (:rtree/root (find-tree (d/db %1))) %2))
        search-box (bbox/extents 0.0 0.0 10.0 10.0)
        conn (conn-fn)]
    (build-tree conn)
    (println "----------------------")
    (println "--# Search Timings #--")
    (time-search conn naive-fn search-box
                 "naive-intersecting and bulk/dyn-cost-partition.")
    (time-search conn rtree-fn search-box
                 "rtree/intersecting and bulk/dyn-cost-partition.")
    :done))

(defn timings [num-entries max-children min-children]
  (let [uri "datomic:mem://rtrees"
        create-db #(create-and-connect-db uri
                                          "resources/datomic/schema.edn"
                                          "resources/datomic/geojsonschema.edn")]
    (install-timings create-db num-entries max-children min-children)
    (tree-build-timings create-db num-entries max-children min-children)
    (search-timings create-db num-entries max-children min-children)))

