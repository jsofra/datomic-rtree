(ns meridian.datomic-rtree.shapes-test
  (:require [datomic.api :as d]
            [clojure.test :refer :all]
            [meridian.datomic-rtree.shapes :refer :all]))

(deftest test-shape->ent
  (let [add-id #(assoc % :db/id 1)
        shape {:type :FeatureCollection
               :crs {:type :link
                     :properties {:href "http://example.com/crs/42"
                                  :type "proj4"}}
               :features [{:type :Feature
                           :geometry {:type :Point
                                      :coordinates [102.0 0.5]}
                           :properties {:prop0 "value0"}}
                          {:type :Feature
                           :geometry {:type :LineString
                                      :coordinates [[102.0 0.0] [103.0 1.0]
                                                    [104.0 0.0] [105.0 1.0]]}
                           :properties {:prop0 "value0"
                                        :prop1 0.0}}
                          {:type :Feature
                           :geometry {:type :Polygon
                                      :coordinates [[[100.0 0.0] [101.0 0.0]
                                                     [101.0 1.0] [100.0 1.0] [100.0 0.0]]]}
                           :properties {:prop0 "value0"
                                        :prop1 1.0}}]}
        ent {:type :FeatureCollection
             :db/id 1
             :crs {:type :link
                   :db/id 1
                   :properties "{:href \"http://example.com/crs/42\", :type \"proj4\"}\n"}
             :features [{:type :Feature
                         :db/id 1
                         :geometry {:type :Point
                                    :db/id 1
                                    :coordinates "[102.0 0.5]\n"}
                         :properties {:db/id 1
                                      :prop0 "value0"}}
                        {:type :Feature
                         :db/id 1
                         :geometry {:type :LineString
                                    :db/id 1
                                    :coordinates "[[102.0 0.0] [103.0 1.0] [104.0 0.0] [105.0 1.0]]\n"}
                         :properties {:db/id 1
                                      :prop0 "value0"
                                      :prop1 0.0}}
                        {:type :Feature
                         :db/id 1
                         :geometry {:type :Polygon
                                    :db/id 1
                                    :coordinates "[[[100.0 0.0] [101.0 0.0] [101.0 1.0] [100.0 1.0] [100.0 0.0]]]\n"}
                         :properties {:db/id 1
                                      :prop0 "value0"
                                      :prop1 1.0}}]}]
    (is (= (->ent shape add-id) ent))))

(deftest test-ent->shape
  (let [shape {:type :FeatureCollection
               :db/id 1
               :crs {:type :link
                     :db/id 1
                     :properties {:href "http://example.com/crs/42"
                                  :type "proj4"}}
               :features [{:type :Feature
                           :db/id 1
                           :geometry {:type :Point
                                      :db/id 1
                                      :coordinates [102.0 0.5]}
                           :properties {:db/id 1
                                        :prop0 "value0"}}
                          {:type :Feature
                           :db/id 1
                           :geometry {:type :LineString
                                      :db/id 1
                                      :coordinates [[102.0 0.0] [103.0 1.0]
                                                    [104.0 0.0] [105.0 1.0]]}
                           :properties {:db/id 1
                                        :prop0 "value0"
                                        :prop1 0.0}}
                          {:type :Feature
                           :db/id 1
                           :geometry {:type :Polygon
                                      :db/id 1
                                      :coordinates [[[100.0 0.0] [101.0 0.0]
                                                     [101.0 1.0] [100.0 1.0] [100.0 0.0]]]}
                           :properties {:db/id 1
                                        :prop0 "value0"
                                        :prop1 1.0}}]}
        ent {:type :FeatureCollection
             :db/id 1
             :crs {:type :link
                   :db/id 1
                   :properties "{:href \"http://example.com/crs/42\", :type \"proj4\"}\n"}
             :features [{:type :Feature
                         :db/id 1
                         :geometry {:type :Point
                                    :db/id 1
                                    :coordinates "[102.0 0.5]\n"}
                         :properties {:db/id 1
                                      :prop0 "value0"}}
                        {:type :Feature
                         :db/id 1
                         :geometry {:type :LineString
                                    :db/id 1
                                    :coordinates "[[102.0 0.0] [103.0 1.0] [104.0 0.0] [105.0 1.0]]\n"}
                         :properties {:db/id 1
                                      :prop0 "value0"
                                      :prop1 0.0}}
                        {:type :Feature
                         :db/id 1
                         :geometry {:type :Polygon
                                    :db/id 1
                                    :coordinates "[[[100.0 0.0] [101.0 0.0] [101.0 1.0] [100.0 1.0] [100.0 0.0]]]\n"}
                         :properties {:db/id 1
                                      :prop0 "value0"
                                      :prop1 1.0}}]}]
    (is (= (->shape ent) shape))))
