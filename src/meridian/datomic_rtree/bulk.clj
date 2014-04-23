(ns meridian.datomic-rtree.bulk
  (:require [meridian.datomic-rtree.bbox :as bbox]
            [meridian.datomic-rtree.rtree :as rtree]))

(defn min-cost-index [ents min-children cost-fn]
  (->> ents
       (reductions bbox/union)
       (drop (dec min-children))
       (map cost-fn)
       (map-indexed vector)
       (apply min-key second)
       first
       (+ min-children)))

(defn cost-partition [ents max-children min-children cost-fn]
  (loop [buckets [] rem ents]
    (if (<= (count rem) max-children)
      (conj buckets rem)
      (let [min-index (-> (take max-children rem)
                          (min-cost-index min-children cost-fn))]
        (recur (conj buckets (take min-index rem)) (drop min-index rem))))))

(defn p-cost-partition
  [ents max-children min-children cost-fn n-splits]
  (apply concat
         (pmap #(cost-partition % max-children min-children cost-fn)
               (partition-all (Math/round (double (/ (count ents) n-splits))) ents))))

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

(defn bulk-tx [ents tree-id max-children min-children cost-fn]
  (let [partiioner #(dyn-cost-partition % max-children min-children cost-fn)
        levels (create-levels ents partiioner max-children)
        ents (flatten (rest levels))]
    (conj ents {:db/id tree-id
                :rtree/root (-> ents last :db/id)
                :rtree/max-children max-children
                :rtree/min-children min-children})))
