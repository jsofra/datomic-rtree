(ns meridian.datomic-rtree.bbox)

(defn bbox [x y w h]
  {:bbox/max-x (+ x w) :bbox/min-x x
   :bbox/max-y (+ y h) :bbox/min-y y})

(defn extents [x1 y1 x2 y2]
  {:bbox/max-x (max x1 x2) :bbox/min-x (min x1 x2)
   :bbox/max-y (max y1 y2) :bbox/min-y (min y1 y2)})

(defn area [{max-x :bbox/max-x min-x :bbox/min-x
             max-y :bbox/max-y min-y :bbox/min-y}]
  (* (- max-x min-x) (- max-y min-y)))

(defn centre [{max-x :bbox/max-x min-x :bbox/min-x
               max-y :bbox/max-y min-y :bbox/min-y}]
  [(* 0.5 (+ max-x min-x)) (* 0.5 (+ max-y min-y))])

(defn union [bbox-a bbox-b]
  (let [{amax-x :bbox/max-x amin-x :bbox/min-x
         amax-y :bbox/max-y amin-y :bbox/min-y} bbox-a
         {bmax-x :bbox/max-x bmin-x :bbox/min-x
          bmax-y :bbox/max-y bmin-y :bbox/min-y} bbox-b]
    (extents (max amax-x bmax-x) (max amax-y bmax-y)
                  (min amin-x bmin-x) (min amin-y bmin-y))))

(defn enlargement [enlarge with]
  (- (area (union enlarge with))
     (area enlarge)))

(defn intersects? [bbox-a bbox-b]
  (let [{amax-x :bbox/max-x amin-x :bbox/min-x
         amax-y :bbox/max-y amin-y :bbox/min-y} bbox-a
         {bmax-x :bbox/max-x bmin-x :bbox/min-x
          bmax-y :bbox/max-y bmin-y :bbox/min-y} bbox-b]
    (and (>= amax-x bmin-x) (<= amin-x bmax-x)
         (>= amax-y bmin-y) (<= amin-y bmax-y))))

(defn contains? [bbox-a bbox-b]
  (let [{amax-x :bbox/max-x amin-x :bbox/min-x
         amax-y :bbox/max-y amin-y :bbox/min-y} bbox-a
         {bmax-x :bbox/max-x bmin-x :bbox/min-x
          bmax-y :bbox/max-y bmin-y :bbox/min-y} bbox-b]
    (and (>= amax-x bmax-x) (<= amin-x bmin-x)
         (>= amax-y bmax-y) (<= amin-y bmin-y))))

(defn enlargement-fn [with]
  (fn [enlarge] (enlargement enlarge with)))

(defn group [group] (reduce union group))
