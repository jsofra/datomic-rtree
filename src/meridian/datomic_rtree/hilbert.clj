(ns meridian.datomic-rtree.hilbert)

(defn mirror-x [[a b c d]] [c d a b])
(defn rotate-c [[a b c d]] [c a d b])
(defn rotate-cc [[a b c d]] [b d a c])

(defn index-fn [r [s e]]
  (let [[f0 fh f1] (map #(+ (* (- e s) %) s) [0.0 0.5 1.0])]
    (fn hilberg-index*
      ([p] (hilberg-index* p r))
      ([p r] (hilberg-index* p r [0.0 0.25 0.75 0.5]))
      ([[x y] r [a b c d :as curve]]
         (if (<= r 0)
           0.0
           (cond
            (and (>= x f0) (<= x fh) (>= y f0) (<= y fh))
            (+ a (/ (hilberg-index* [(* x 2) (* y 2)] (dec r)
                                    (-> curve mirror-x rotate-c)) 4))
            (and (>= x fh) (<= x f1) (>= y f0) (<= y fh))
            (+ b (/ (hilberg-index* [(* (- x fh) 2) (* y 2)] (dec r) curve) 4))
            (and (>= x f0) (<= x fh) (>= y fh) (<= y f1))
            (+ c (/ (hilberg-index* [(* x 2) (* (- y fh) 2)] (dec r)
                                    (-> curve mirror-x rotate-cc)) 4))
            (and (>= x fh) (<= x f1) (>= y fh) (<= y f1))
            (+ d (/ (hilberg-index* [(* (- x fh) 2) (* (- y fh) 2)] (dec r) curve) 4))))))))

(defn h-index
  ([p] (h-index p (bit-shift-left 1 31)))
  ([[x y] mask]
     (let [not-y (bit-not (bit-xor y x))]
       (loop [m mask h 0 x x]
         (let [m' (bit-shift-right (bit-and 0xFFFFFFFF m) 1)
               [h' x'] (if (not (zero? (bit-and y m)))
                         (if (zero? (bit-and x m))
                           [(bit-or (bit-shift-left h 2) 1) x]
                           [(bit-or (bit-shift-left h 2) 3) (bit-xor x not-y)])
                         (if (zero? (bit-and x m))
                           [(bit-shift-left h 2) (bit-xor x y)]
                           [(bit-or (bit-shift-left h 2) 2) x]))]
           (if (zero? m') h (recur m' h' x')))))))
