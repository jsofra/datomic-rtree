(ns meridian.datomic-rtree.rtree
  (:use [datomic.api :only (q db) :as d])
  (:require [meridian.datomic-rtree.bbox :as bbox])
  (:require [clojure.core.reducers :as r]))

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

(defn pick-next
  "Select one remaining entry for classification in a group.
   Determine the cost of putting each entry in each group.
   Find the entry with greatest preference for one group."
  [group-a group-b nodes]
  (let [bboxes (map bbox/group [group-a group-b])
        cost-diff (fn [node]
                    (let [enlargements (map (bbox/enlargement-fn node) bboxes)]
                      (Math/abs (apply - enlargements))))]
    (apply max-key cost-diff nodes)))

(defn group-chooser
  "For any given node returns a function that given a group will
   return a value that can be used as a sort order for the group."
  [node]
  (fn [group]
    (let [bbox (bbox/group group)]
      [(bbox/enlargement bbox node) (bbox/area bbox) (count group)])))

(defn split-accum
  "Produces a lazy seq of vectors containing a list of groups (that accumulate
   at every step) and a list of children yet to be grouped at that step."
  [init-groups children]
  (iterate (fn [[[group1 group2 :as groups] remaining]]
             (let [picked (pick-next group1 group2 remaining)
                   [group1 group2] (sort-by (group-chooser picked) groups)]
               [(sort-by count [(conj group1 picked) group2])
                (disj remaining picked)]))
           [init-groups children]))

(defn split-finished? [min-children [groups remaining]]
  (and (seq remaining)
       (> (+ (count remaining) (count (first groups))) min-children)))

(defn split-node
  "Quadractic-Cost Algorithm. Attempts to find a small-area split.
   Given a set of children (and a minimum number of children allowed in a node),
   returns a vector containing two new sets of child nodes."
  [children min-children]
  (let [[seed-a seed-b] (pick-seeds children)
        split-seq (split-accum [#{seed-a} #{seed-b}] (disj children seed-a seed-b))
        final (first (drop-while (partial split-finished? min-children) split-seq))
        [[group1 group2 :as groups] remaining] final]
    (if (empty? remaining)
      groups
      [(clojure.set/union group1 remaining) group2])))

;; convert the tree adjustments into transactions

(defn node-ids [nodes] (set (map :db/id nodes)))

(defn add-id [node] (assoc (into {} node) :db/id (d/tempid :db.part/user)))

(defn new-splits [node old-child new-children min-children id-adder]
  (let [split-children (-> (:node/children node)
                           (disj old-child)
                           (clojure.set/union (set new-children))
                           (split-node min-children))]
    (map #(-> % bbox/group
              id-adder
              (assoc :node/children %)) split-children)))

(defn adjusted-node [node entry new-children]
  (cond-> (assoc (bbox/union entry node) :db/id (:db/id node))
          (seq new-children) (assoc :node/children (node-ids new-children))))

(defn split-tx [node split]
  (let [split (map #(merge (select-keys node [:node/is-leaf?]) %) split)]
    (conj (map #(assoc % :node/children (node-ids (:node/children %))) split)
          [:db.fn/retractEntity (:db/id node)])))

(defn grow-tree-tx [tree-id split]
  (let [new-root-id (d/tempid :db.part/user)]
    [[:db/add tree-id :rtree/root new-root-id]
     (merge (bbox/group split)
            {:db/id new-root-id :node/children (node-ids split)})]))

(defn should-split? [node split-nodes {max-children :rtree/max-children}]
  (and (= (count (:node/children node)) max-children)
       (seq split-nodes)))

(defn add-split-tx [txs parent? node new-splits tree]
  (concat txs (split-tx node new-splits)
          (when parent?
            (grow-tree-tx (:db/id tree) new-splits))))

(defn add-adjust-tx [txs node entry split-nodes]
  (conj txs (adjusted-node node entry split-nodes)))

(defn insert-entry-tx [tree entry & {:keys [install-entry] :or {install-entry false}}]
  (loop [node (-> (:rtree/root tree)
                  (choose-leaf entry))
         prev-node nil
         split-nodes #{entry}
         txs (if install-entry [entry] [])]
    (let [parent (first (:node/_children node))
          new-split-nodes (delay (new-splits node prev-node split-nodes
                                             (:rtree/min-children tree) add-id))
          split? (should-split? node split-nodes tree)
          new-txs (if split?
                    (add-split-tx txs (nil? parent) node @new-split-nodes tree)
                    (add-adjust-tx txs node entry split-nodes))]
      (if parent
        (recur parent node (if split? @new-split-nodes #{}) new-txs)
        new-txs))))

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

(defn containing
  "Given the root of the tree and a bounding box to search within finds entries
   contained within the search box."
  [root search-box]
  (search-tree root
               (fn [children]
                 (filter #(bbox/contains? search-box %) children))
                (fn [node children recur-fn]
                  (if (= (count children) (count (:node/children node)))
                    (search-tree node identity)
                    recur-search-step))))

(defn hilbert-ents [db]
  (->> (d/seek-datoms db :avet :bbox/hilbert-val)
       (map #(d/entity db (:e %)))
       (filter :bbox/hilbert-val)))
