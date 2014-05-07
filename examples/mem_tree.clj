(ns mem-tree
  (:use [datomic.api :only (q db) :as d]
        quil.core)
  (:require [meridian.datomic-rtree.test-utils :as utils]
            [meridian.datomic-rtree.rtree :as rtree]
            [meridian.datomic-rtree.bbox :as bbox]
            [meridian.datomic-rtree.bulk :as bulk])
  (:import datomic.Util))

(def uri "datomic:mem://rtrees")

(defn print-tree [conn]
  (let [root (:rtree/root (utils/find-tree (d/db conn)))]
    ((fn walk [n indent]
       (println (str indent (:db/id n) " " (:node/entry n)))
       (doseq [c (:node/children n)]
         (walk c (str indent "---"))))
     root "")))

(defn install-and-print-tree [conn num-entries]
  (utils/install-rand-data conn num-entries utils/create-feature)
  (print-tree conn))

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

(comment
  (def conn (utils/create-and-connect-db uri
                                         "resources/datomic/schema.edn"
                                         "resources/datomic/geojsonschema.edn"))
  (utils/install-rand-data conn 1000 utils/create-feature)
  (utils/install-rand-ents conn 1000 utils/create-feature)
  (utils/create-tree-and-install-rand-data conn 40 6 3)
  (def search-box (bbox/extents 0.0 0.0 10.0 10.0))
  (def root (:rtree/root (find-tree (d/db conn))))
  (time (count (naive-intersecting (all-entries (d/db conn)) search-box)))
  (time (count (rtree/intersecting root search-box)))
  (time (count (d/q intersecting-q (d/db conn) search-rules (:db/id root) search-box)))
  (time (utils/install-and-bulk-load conn 10000 6 3))
  )

;;;;;; draw tree with quill ;;;;;;

(defn all-bbox [db]
  (map #(d/entity db (first %))
       (d/q '[:find ?e :where [?e :bbox/min-x]] db)))

(defn key-press []
  (let [conn (state :conn)]
    (install-and-print-tree conn 1)
    (reset! (state :rects) (all-bbox (d/db conn)))))

(defn setup-sketch []
  (frame-rate 30)
  (smooth)
  (let [conn (utils/create-and-connect-db uri
                                          "resources/datomic/schema.edn"
                                          "resources/datomic/geojsonschema.edn")]
    #_(do (utils/install-rand-ents conn 30 utils/create-feature)
          (utils/bulk-load-ents conn 6 3 bulk/dyn-cost-partition))

    (do (utils/create-tree conn 6 3)
        (utils/install-rand-ents conn 1 utils/create-feature)
        (utils/load-ents conn 6 3))

    (set-state! :conn conn
                :rects (atom (all-bbox (d/db conn))))))

(defn draw-sketch []
  (stroke 255)
  (fill 255)
  (rect 0 0 600 600)

  (doseq [r (sort-by :node/is-leaf? @(state :rects))]
    (no-fill)
    (stroke-weight 1)
    (cond
     (:node/entry r) (do (stroke-weight 2) (stroke 0 0 256))
     (:node/is-leaf? r) (do (stroke-weight 2) (stroke 256 0 0))
     :else (do (stroke-weight 2) (stroke 20 180 200)))
    (rect (:bbox/min-x r) (:bbox/min-y r)
          (- (:bbox/max-x r) (:bbox/min-x r)) (- (:bbox/max-y r) (:bbox/min-y r)))))

(defn tree-sketch []
  (sketch :title "R-tree"
          :setup setup-sketch
          :draw draw-sketch
          :key-typed key-press
          :size [600 600]))
