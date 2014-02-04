(ns meridian.datomic-rtree.rtree
  (:use [datomic.api :only (q db) :as d])
  (:require [meridian.datomic-rtree.bbox :as bbox]))

(defn choose-leaf
  "Select a leaf node in which to place a new index entry, new-bbox.
   Starts at the root node a works down the tree. Finds the node whose
   path to the root consists of ancestors nodes that all required the
   least enlargement to include new-bbox, or have the smallest area."
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
   Determine the cost of putting each entry in each group.
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

(defn add-id [node] (assoc (into {} node) :db/id (d/tempid :db.part/user)))

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
  (let [new-root-id (d/tempid :db.part/user)]
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

(defn create-tree-tx [max-children min-children]
  [[:rtree/construct #db/id[:db.part/user] max-children min-children]])

;; basic search functions

(defn- recur-search-step [node children recur-fn]
  (if (:node/is-leaf? node)
   children
   (mapcat #(lazy-seq (recur-fn %)) children)))

(defn search-tree
  ([root child-filter] (search-tree root child-filter recur-search-step))
  ([root child-filter recur-step]
     ((fn step [node]
        (let [children (child-filter (:node/children node))]
          (recur-step node children step))) root)))

(defn intersecting
  "Given the root of the tree and a bounding box to search within finds entries
   intersecting the search box."
  [root search-box]
  (search-tree root
               (fn [children]
                 (filter #(bbox/intersects? search-box %) children))))

(defn containing [root search-box]
  "Given the root of the tree and a bounding box to search within finds entries
   contained within the search box."
  (search-tree root
               (fn [children]
                 (filter #(bbox/contains? search-box %) children))
                (fn [node children recur-fn]
                  (if (= (count children) (count (:node/children node)))
                    (search-tree node identity)
                    recur-search-step))))

(defn hilbert-ents [db]
  (->> (d/seek-datoms db :avet :node/hilbert-val)
       (map #(d/entity db (:e %)))))
