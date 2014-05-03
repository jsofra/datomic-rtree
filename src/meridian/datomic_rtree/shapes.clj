(ns meridian.datomic-rtree.shapes
  (:require [meridian.datomic-rtree.rtree :as rtree]
            [meridian.datomic-rtree.bbox :as bbox]))

(defn transform-shape [shape f]
  (let [transformed (f shape)
        updater #(transform-shape % f)
        update-coll (fn [k] (update-in transformed [k] #(mapv updater %)))]
    (case (:type shape)
      :Feature (update-in transformed [:geometry] updater)
      :GeometryCollection (update-coll :geometries)
      :FeatureCollection (update-coll :features)
      transformed)))

(defn transform-map [k f m]
  (if (contains? m k) (f m) m))

(defn transform-nested-map [k f]
  (partial transform-map k #(update-in % [k] f)))

(defn update-entry [k f m]
  (transform-map k #(update-in % [k] f) m))

(defn read-entry [k m]
  (update-entry k clojure.edn/read-string m))

(defn prn-entry [k m]
  (update-entry k prn-str m))

(defn ->shape [ent]
  (transform-shape ent
                   (comp
                    (partial read-entry :bbox)
                    (partial read-entry :coordinates)
                    (transform-nested-map :crs (partial read-entry :properties)))))

(defn ->ent
  ([shape] (->ent shape rtree/add-id))
  ([shape add-id]
     (transform-shape shape
                      (comp
                       (partial prn-entry :bbox)
                       (partial prn-entry :coordinates)
                       (partial transform-map :type add-id)
                       (transform-nested-map :crs add-id)
                       (transform-nested-map :crs (partial prn-entry :properties))
                       (transform-nested-map :properties add-id)))))

(defn create-entry [shape index-fn]
  (let [box (apply bbox/extents (:bbox shape))]
    (rtree/add-id
     (merge box
            {:node/entry (->ent shape)
             :bbox/hilbert-val (index-fn (bbox/centre box))}))))
 
