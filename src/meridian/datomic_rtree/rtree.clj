(ns meridian.datomic-rtree.rtree
  (:use [datomic.api :only (q db) :as d])
  (:require [clojure.java.io :as io]
            [meridian.datomic-rtree.bbox :as bbox])
  (:import datomic.Util))

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
                  (sort-by (juxt (bbox/enlargement-fn new-bbox) bbox/area))
                  first)))))


(defn pick-seeds
  "Select two entries to be the first elements of the new groups.
   Choose the most wasteful (in terms of area) pair."
  [entries]
  (let [pairs (for [x entries y entries :when (not= x y)] [x y])
        innefficiency (fn [[e1 e2]]
                        (- (bbox/area (bbox/union e1 e2))
                           (bbox/area e1)
                           (bbox/area e2)))]
    (apply max-key innefficiency pairs)))

(defn pick-next [group-a group-b nodes]
  "Select one remaining entry for classification in a group.
   Determine te cost of putting each entry in each group.
   Find the entry with greatest preference for one group."
  (let [bboxes (map bbox/group [group-a group-b])
        cost-diff (fn [node]
                    (let [enlargements (map (bbox/enlargement-fn node) bboxes)]
                      (Math/abs (apply - enlargements))))]
    (apply max-key cost-diff nodes)))

(defn group-chooser [node]
  (fn [group]
    (let [bbox (bbox/group group)]
      [(bbox/enlargement bbox node) (bbox/area bbox) (count group)])))

(defn split-node [children min-children]
  "Quadractic-Cost Algorithm. Attempts to find a small-area split.
   Given a set of children (and a minimum number of children allowed in a node),
   returns a vector containing two new sets of child nodes."
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

;; convert the tree adjustments into transactions

(defn node-ids [nodes] (set (map :db/id nodes)))

(defn add-id [node] (assoc node :db/id (d/tempid :db.part/user)))

(defn new-splits [node old-child new-children min-children id-adder]
  (let [split-children (-> (:node/children node)
                           (disj old-child)
                           (clojure.set/union (set new-children))
                           (split-node min-children))
        bboxes (map (comp id-adder bbox/group) split-children)]
    (map #(assoc %1 :node/children %2) bboxes split-children)))

(defn adjusted-node [node entry new-children]
  (cond-> (assoc (bbox/union entry node) :db/id (:db/id node))
          (seq new-children) (assoc :node/children (node-ids new-children))))

(defn split-tx [node split]
  (let [split (map #(merge (select-keys node [:node/is-leaf?]) %) split)]
    (conj (map #(assoc % :node/children (node-ids (:node/children %))) split)
          [:db.fn/retractEntity (:db/id node)])))

(defn node-op [node prev-node split-nodes entry
               {max-children :rtree/max-children min-children :rtree/min-children}]
  (if (and (= (count (:node/children node)) max-children) (seq split-nodes))
    {:split (new-splits node prev-node split-nodes min-children add-id)}
    {:adjust (adjusted-node node entry split-nodes)}))

(defn grow-tree-tx [tree-id split]
  (let [new-root-id #db/id[:db.part/user]]
    [[:db/add tree-id :rtree/root new-root-id]
     (merge (bbox/group split)
            {:db/id new-root-id :node/children (node-ids split)})]))

(defn insert-entry-tx [tree entry]
  (let [new-entry (add-id entry)
        leaf (-> (:rtree/root tree)
                 (choose-leaf new-entry))]
    (loop [node leaf
           prev-node nil
           split-nodes #{new-entry}
           txs [new-entry]]
      (let [parent (first (:node/_children node))
            {:keys [split adjust]} (node-op node prev-node split-nodes new-entry tree)
            txs (if split
                  (concat txs (split-tx node split)
                          (when (nil? parent)
                            (grow-tree-tx (:db/id tree) split)))
                  (conj txs adjust))]
        (if parent
          (recur parent node (or split #{}) txs)
          txs)))))


(def uri "datomic:mem://rtrees")

(defn find-tree [db]
  (->> (d/q '[:find ?e :where [?e :rtree/root]] db)
       ffirst (d/entity db)))

(defn create-tree
  ([] (create-tree 50 20))
  ([max-children min-children]
     [[:rtree/construct #db/id[:db.part/user] max-children min-children]]))

(defn create-and-connect-db
  ([uri schema] (create-and-connect-db uri schema 50 20))
  ([uri schema max-children min-children]
     (d/delete-database uri)
     (d/create-database uri)
     (let [conn (d/connect uri)]
       (->> (read-string (slurp schema))
            (d/transact conn))
       (d/transact conn (create-tree max-children min-children))
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
                         (assoc (apply bbox/extents box) :node/entry entry)]]))))

(defn install-rand-data [conn num-entries]
  (let [test-data (into [] (take num-entries (partition 4 (repeatedly #(rand 500)))))]
    (time (doseq [box test-data]
            (d/transact conn [[:rtree/insert-entry
                               (assoc (apply bbox/extents box)
                                 :node/entry (str (char (rand-int 200))))]])))))

;(def conn (create-and-connect-db uri "resources/datomic/schema.edn"))
;(install-test-data conn)
;(->> (find-tree (d/db conn)) :rtree/root :node/children)
;(time (count (d/q '[:find ?e :in $ :where [?e :node/entry] [(datomic.api/entity $ ?e) ?b] [(meridian.datomic-rtree.bbox/intersect? (meridian.datomic-rtree.bbox/extents 0.0 0.0 100.0 100.0) ?b)]] (d/db conn))))
;(time (count (d/q search (d/db conn) rules (bbox-extents 0.0 0.0 100.0 100.0))))
;(time (count (intersects? (:rtree/root (find-tree (d/db conn))) (bbox/extents 0.0 0.0 100.0 100.0))))



(defn print-tree [conn]
  (let [root (:rtree/root (find-tree (d/db conn)))]
    ((fn walk [n indent]
       (println (str indent (:db/id n) " " (:node/entry n)))
       (doseq [c (:node/children n)]
         (walk c (str indent "---"))))
     root "")))

(defn intersects? [root bbox]
  ((fn step [node]
         (let [children (filter #(bbox/intersect? bbox %) (:node/children node))]
           (if (:node/is-leaf? node)
             children
             (concat (mapcat step children))))) root))

(def rules '[[(parent ?a ?b)
              [?a :node/children ?b]]
             [(ancestor ?a ?b)
              [parent ?a ?b]]
             [(ancestor ?a ?b)
              [parent ?a ?x]
              [ancestor ?x ?b]]
             [(intersects ?e ?search-box)
              [ancestor ?x ?e]
              [?z :rtree/root ?x]
              #_[intersects ?x ?search-box]]
             [(intersects ?e ?search-box)
              [(datomic.api/entity $ ?e) ?eb]
              [(meridian.datomic-rtree.rtree/bbox-intersect? ?eb ?search-box)]]
             ])

(def search '[:find ?e :in $ % ?b
              :where
              [?e :node/entry]
              [intersects ?e ?b]])
