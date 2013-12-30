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

(defn bbox-adjustments [path entry]
  (map (juxt identity (partial bbox-union entry)) path))

(defn split-adjustments [path entry min-children id-adder]
  (rest (reductions
         (fn [[old-child new-children] node]
           (let [split-children (-> (:node/children node)
                                    (disj old-child)
                                    (clojure.set/union (set new-children))
                                    (split-node min-children))
                 bboxes (map (comp id-adder group-bbox) split-children)]
             [node (map #(assoc %1 :node/children %2) bboxes split-children)]))
         [nil #{entry}] path)))

(defn tree-adjustments [path-from-leaf entry id-adder
                        {max-children :rtree/max-children
                         min-children :rtree/min-children}]
  (let [split? #(= (count (:node/children %)) max-children)]
    {:bbox-adj (bbox-adjustments (drop-while split? path-from-leaf) entry)
     :split-adj (split-adjustments (take-while split? path-from-leaf)
                                           entry min-children id-adder)}))

;; convert the tree adjustments into transactions

(defn bbox-adj-tx [bbox-adj new-entry]
  (for [[node bbox] bbox-adj]
       (cond-> (assoc bbox :db/id (:db/id node))
               (:node/is-leaf? node) (assoc :node/children #{(:db/id new-entry)}))))

(defn node-ids [nodes] (set (map :db/id nodes)))

(defn split-adj-tx [split-adj]
  (mapcat
   (fn [[node split]]
     (let [split (map #(merge (select-keys node [:node/is-leaf?]) %) split)]
       (conj (map #(assoc % :node/children (node-ids (:node/children %))) split)
             [:db.fn/retractEntity (:db/id node)])))
          split-adj))

(defn grow-tree-tx [tree-id split-adj]
  (let [[_ split] (last split-adj)
        new-root-id #db/id[:db.part/user]]
    [[:db/add tree-id :rtree/root new-root-id]
     (merge (group-bbox split)
            {:db/id new-root-id :node/children (node-ids split)})]))

(defn connect-adj-tx [bbox-adj split-adj]
  (let [[_ split] (last split-adj)]
    [[:db/add (:db/id (ffirst bbox-adj)) :node/children (node-ids split)]]))

(defn add-id [node] (assoc node :db/id (d/tempid :db.part/user)))

(defn insert-entry-tx [tree entry]
  (let [new-entry (add-id entry)
        {:keys [bbox-adj split-adj]}
        (-> (:rtree/root tree)
            (choose-leaf-path new-entry)
            reverse
            (tree-adjustments new-entry add-id tree))]
    (concat
     [new-entry]

     (if (empty? bbox-adj)
       (grow-tree-tx (:db/id tree) split-adj)
       (when (seq split-adj)
         (connect-adj-tx bbox-adj split-adj)))

     (bbox-adj-tx bbox-adj new-entry)
     (split-adj-tx split-adj))))

(def uri "datomic:mem://rtrees")

(defn find-tree [db]
  (->> (d/q '[:find ?e :where [?e :rtree/root]] db)
       ffirst (d/entity db)))

(defn create-tree
  ([] (create-tree 50 20))
  ([max-children min-children]
     [[:rtree/construct #db/id[:db.part/user] max-children min-children]]))

(defn create-and-connect-db [uri schema]
  (d/delete-database uri)
  (d/create-database uri)
  (let [conn (d/connect uri)]
    (->> (read-string (slurp schema))
       (d/transact conn))
    (d/transact conn (create-tree))
    ;(install-test-data conn)
    conn))

(defn install-test-data [conn]
  (let [test-data [[[0.0 0.0 10.0 10.0] "a"]
                   [[5.0 5.0 8.0 8.0] "b"]
                   [[0.0 0.0 20.0 20.0] "c"]
                   [[2.0 2.0 6.0 6.0] "d"]
                   [[-10.0 -5.0 5.0 5.0] "e"]
                   [[-100.0 -50.0 5.0 5.0] "f"]
                   [[-1.0 -5.0 50.0 50.0] "g"]
                   [[1.0 1.0 8.0 8.0] "h"]]]
    (doseq [[bbox entry] test-data]
      (d/transact conn [[:rtree/insert-entry
                         (assoc (apply bbox-extents bbox) :node/entry entry)]]))))

(defn install-rand-data [conn num-entries]
  (let [test-data (into [] (take num-entries (partition 4 (repeatedly #(rand 500)))))]
    (time (doseq [bbox test-data]
            (d/transact conn [[:rtree/insert-entry
                               (assoc (apply bbox-extents bbox)
                                 :node/entry (str (char (rand-int 200))))]])))))

;(def conn (create-and-connect-db uri "resources/datomic/schema.edn"))
;(install-test-data conn)
;(->> (find-tree (d/db conn)) :rtree/root :node/children)

(defn print-tree [conn]
  (let [root (:rtree/root (find-tree (d/db conn)))]
    ((fn walk [n indent]
       (println (str indent (:db/id n) " " (:node/entry n)))
       (doseq [c (:node/children n)]
         (walk c (str indent "---"))))
     root "")))
