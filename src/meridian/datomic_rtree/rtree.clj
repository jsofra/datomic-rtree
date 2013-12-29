(ns meridian.datomic-rtree.rtree
  (:use [datomic.api :only (q db) :as d])
  (:require [clojure.java.io :as io])
  (:import datomic.Util))

(defn bbox-extents [x1 y1 x2 y2]
  {:bbox/max-x (max x1 x2) :bbox/min-x (min x1 x2)
   :bbox/max-y (max y1 y2) :bbox/min-y (min y1 y2)})

(defn bbox-area [{max-x :bbox/max-x min-x :bbox/min-x
                  max-y :bbox/max-y min-y :bbox/min-y}]
  (* (- max-x min-x) (- max-y min-y)))

(defn bbox-union [bbox-a bbox-b]
  (let [{amax-x :bbox/max-x amin-x :bbox/min-x
         amax-y :bbox/max-y amin-y :bbox/min-y} bbox-a
         {bmax-x :bbox/max-x bmin-x :bbox/min-x
          bmax-y :bbox/max-y bmin-y :bbox/min-y} bbox-b]
    (bbox-extents (max amax-x bmax-x) (max amax-y bmax-y)
                  (min amin-x bmin-x) (min amin-y bmin-y))))

(defn bbox-enlargement [enlarge with]
  (- (bbox-area (bbox-union enlarge with))
     (bbox-area enlarge)))

(defn enlargement-fn [with]
  (fn [enlarge] (bbox-enlargement enlarge with)))

(defn choose-leaf
  "Select a leaf node in which to place a new index entry, new-bbox.
   Starts at the root node a works down the tree.
   Chooses a leaf node whose path to the root consists of ancestors nodes
   that all required the least enlargement to include new-bbox, or have
   the smallest area."
  [root new-bbox]
  (loop [n root]
    (if (:node/is-leaf? n)
      n
      (recur (->> (:node/children n)
                  (sort-by (juxt (enlargement-fn new-bbox) bbox-area))
                  first)))))

(defn choose-leaf-path
  "Select a leaf node in which to place a new index entry, new-bbox.
   Starts at the root node a works down the tree.
   Chooses a leaf node whose path to the root consists of ancestors nodes
   that all required the least enlargement to include new-bbox, or have
   the smallest area."
  [root new-bbox]
  (loop [path [root]]
    (if (:node/is-leaf? (last path))
      path
      (recur (->> (:node/children (last path))
                  (sort-by (juxt (enlargement-fn new-bbox) bbox-area))
                  first
                  (conj path))))))

(defn pick-seeds
  "Select two entries to be the first elements of the new groups.
   Choose the most wasteful (in terms of area) pair."
  [entries]
  (let [pairs (for [x entries y entries :when (not= x y)] [x y])
        innefficiency (fn [[e1 e2]]
                        (- (bbox-area (bbox-union e1 e2))
                           (bbox-area e1)
                           (bbox-area e2)))]
    (apply max-key innefficiency pairs)))

(defn group-bbox [group] (reduce bbox-union group))

(defn pick-next [group-a group-b nodes]
  "Select one remaining entry for classification in a group.
   Determine te cost of putting each entry in each group.
   Find the entry with greatest preference for one group."
  (let [bboxes (map group-bbox [group-a group-b])
        cost-diff (fn [node]
                    (let [enlargements (map (enlargement-fn node) bboxes)]
                      (Math/abs (apply - enlargements))))]
    (apply max-key cost-diff nodes)))

(defn group-chooser [node]
  (fn [group]
    (let [bbox (group-bbox group)]
      [(bbox-enlargement bbox node) (bbox-area bbox) (count group)])))

(defn split-node [children min-children]
  (let [[seed-a seed-b] (pick-seeds children)]
    (loop [groups [#{seed-a} #{seed-b}]
           remaining (disj children seed-a seed-b)]
      (if (empty? remaining)
        groups
        (let [[first-group second-group] (sort-by count groups)]
          (if (<= (+ (count remaining) (count first-group)) min-children)
            [(clojure.set/union first-group remaining) second-group]
            (let [picked (pick-next first-group second-group remaining)
                  [first-group second-group] (sort-by (group-chooser picked) groups)]
              (recur [(conj first-group picked) second-group]
                     (disj remaining picked)))))))))

(defn split-node? [node max-children]
  (= (count (:node/children node)) max-children))

(defn bbox-adjustments* [path-to-leaf entry]
  (let [adjustments (->> (reverse path-to-leaf)
                         (reductions #(bbox-union %1 (group-bbox (:node/children %2))) entry)
                         rest reverse)
        #_(rest (reductions #(bbox-union %1 (group-bbox (:node/children %2)))
                          entry leaf-to-root))
        ]
    (map (partial bbox-union entry) path-to-leaf)
    (map (fn [n a] [n {:adjust a}]) path-to-leaf adjustments)))

(defn bbox-adjustments [path entry]
  (map (juxt identity (partial bbox-union entry)) path))

(defn split-adjustments [path entry min-children]
  (rest (reductions
         (fn [[old-child new-children] node]
           (let [split-children (-> (:node/children node)
                                    (disj old-child)
                                    (clojure.set/union (set new-children))
                                    (split-node min-children))
                 bboxes (map group-bbox split-children)]
             [node (set (map #(assoc %1 :node/children %2) bboxes split-children))]))
         [nil #{entry}] path)))

(defn tree-adjustments [tree path-from-leaf entry]
  (let [split? #(= (count (:node/children %)) (:rtree/max-children tree))
        min-children (:rtree/min-children tree)]
    {:bbox-adjustments (bbox-adjustments (drop-while split? path-from-leaf) entry)
     :split-adjustments (split-adjustments (take-while split? path-from-leaf) entry min-children)}))

(defn insert [tree entry]
  (let [root (:rtree/root tree)
        path-to-leaf (choose-leaf-path root entry)]
    (tree-adjustments tree path-to-leaf entry)))

(def uri "datomic:mem://rtrees")

(def get-root '[:find ?e
                :where [?e :rtree/root]])

(def rtree-rules
  '[[(root-node? ?e ?n) [?e :rtree/root ?n]]

    [(entries ?e ?entires)
     (?e :node/entries ?entires)]])

(defn find-root-node [db]
  (d/entity db
            (ffirst
             (d/q '[:find ?e
                    :where [?e :rtree/root ?n]]
                  db))))

(defn db-fn [db key]
  (:db/fn (d/entity db key)))

(defn create-and-connect-db [uri schema]
  (d/delete-database uri)
  (d/create-database uri)
  (let [conn (d/connect uri)]
    (->> (read-string (slurp schema))
       (d/transact conn))
    (d/transact conn [[:rtree/construct #db/id[:db.part/user] 50 20]])
    ;(install-test-data conn)
    conn))

(defn create-node [conn parent [x1 y1 x2 y2] entry]
  (let [bbox-id #db/id[:db.part/user]
        new-node #db/id[:db.part/user]]
    (d/transact conn [[:bbox/construct bbox-id x1 y1 x2 y2]
                      {:db/id new-node
                       :bbox bbox-id
                       :node/entry entry}
                      [:db/add parent :node/children new-node]])))

(defn install-test-data [conn]
  (let [test-data [[[0.0 0.0 10.0 10.0] "a"]
                   [[5.0 5.0 8.0 8.0] "b"]
                   [[0.0 0.0 20.0 20.0] "c"]
                   [[2.0 2.0 6.0 6.0] "d"]]
        root-node-id (:db/id (:rtree/root (find-root-node (d/db conn))))]
    (doseq [[bbox entry] test-data]
      (create-node conn root-node-id bbox entry))))



;(def conn (create-and-connect-db uri))
;(-> (find-root-node (d/db conn)) :node/children)
;(install-test-data conn)
;(choose-leaf (d/db conn) (find-root-node (d/db conn)) [4.0 4.0 6.0 6.0])
