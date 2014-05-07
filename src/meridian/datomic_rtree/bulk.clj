(ns meridian.datomic-rtree.bulk
  (:require [meridian.datomic-rtree.bbox :as bbox]
            [meridian.datomic-rtree.rtree :as rtree]))

(defn min-cost-index
  "Calculate an index into the list of entities that minimises
   the given cost-fn. Must be at least as large as min-children."
  [entities min-children cost-fn]
  (->> entities
       (reductions bbox/union)
       (drop (dec min-children))
       (map cost-fn)
       (map-indexed vector)
       (apply min-key second)
       first
       (+ min-children)))

(defn partition-accum
  "Produces an accumulating lazy seq of vectors containing a list of
   partitions and a list of entities yet to be partitioned at that step."
  [entities max-children min-children cost-fn]
  (iterate (fn [[partitions remaining]]
             (let [min-index (-> (take max-children remaining)
                                 (min-cost-index min-children cost-fn))]
               [(conj partitions (take min-index remaining))
                (drop min-index remaining)]))
           [[] entities]))

(defn cost-partition
  "Partition the entities up, minimising the cost-fn for each partition."
  [entities max-children min-children cost-fn]
  (->> (partition-accum entities max-children min-children cost-fn)
       (drop-while (fn [[_ rem]] (> (count rem) max-children)))
       first
       (apply conj)))

(defn p-cost-partition
  "Parallel cost-partition function, chunks the data into n-splits."
  [entities max-children min-children cost-fn n-splits]
  (apply concat
         (pmap #(cost-partition % max-children min-children cost-fn)
               (partition-all (quot (count entities) n-splits) entities))))

(defn dyn-cost-partition
  ([ents max-children min-children cost-fn]
     (dyn-cost-partition ents max-children min-children cost-fn
                         (.. Runtime getRuntime availableProcessors)))
  ([ents max-children min-children cost-fn n-splits]
     (if (<= (count ents) (* max-children 2 n-splits))
       (cost-partition ents max-children min-children cost-fn)
       (p-cost-partition ents max-children min-children cost-fn n-splits))))

(defn create-node [children]
  (-> (bbox/group children)
      rtree/add-id
      (assoc :node/children (set (map :db/id children)))))

(defn create-levels [ents partition-fn max-children]
  (let [leaves (partition-fn ents)]
    (loop [levels [leaves] leaves? true]
      (let [node #(cond-> (create-node %2)
                          %1 (assoc :node/is-leaf? true))
            nodes (map (partial node leaves?) (last levels))]
        (if (> (count nodes) max-children)
          (recur (conj levels (partition-fn nodes)) false)
          (-> levels
              (conj nodes)
              (conj [(node false nodes)])))))))

(defn bulk-tx
  ([ents tree-id max-children min-children cost-fn]
     (bulk-tx ents tree-id max-children min-children cost-fn dyn-cost-partition))
  ([ents tree-id max-children min-children cost-fn part-fn]
     (let [partioner #(part-fn % max-children min-children cost-fn)
           levels (create-levels ents partioner max-children)
           ents (flatten (rest levels))]
       (conj ents {:db/id tree-id
                   :rtree/root (-> ents last :db/id)
                   :rtree/max-children max-children
                   :rtree/min-children min-children}))))
