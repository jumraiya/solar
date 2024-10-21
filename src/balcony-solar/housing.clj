(ns balcony-solar.housing
  (:require [nextjournal.clerk :as clerk]
            [tablecloth.api :as tc]
            [clojure.string :as str]
            [clojure.set :as set]
            [clojure.data.json :as json]
            ;[tech.ml.dataset :as tmd]
            ))

;; (defonce data (tc/dataset "data/ahs2021m.csv"))

;; (def data2 (tc/dataset "data/ahs2019m.csv"))
                                        ;(defonce data2 (tc/dataset "data/household.csv"))

(defonce all-data
  (let [data (tc/dataset "data/ahs2021m.csv")
        data2 (tc/dataset "data/ahs2019m.csv")
        col1 (set/difference (set (tc/column-names data)) (set (tc/column-names data2)))
        col2 (set/difference (set (tc/column-names data2)) (set (tc/column-names data)))]
    (tc/union
     (tc/drop-columns (tc/dataset "data/ahs2021m.csv") col1)
     (tc/drop-columns (tc/dataset "data/ahs2019m.csv") col2))))

(defonce data-by-area (tc/group-by all-data "OMB13CBSA" {:result-type :as-map}))

(def metro-housing-units (tc/dataset "data/metro_housing_units.csv"))

(def metro-populations (tc/dataset "data/metro_populations.csv"))
                                        ;(def data3 (tc/dataset "data/ahs2017m.csv"))

;; (def groups (tc/group-by data "OMB13CBSA"))

;; (def groups2 (tc/group-by data2 "OMB13CBSA"))

                                        ;(def groups3 (tc/group-by data3 "OMB13CBSA"))
                                        ;(def metro-area-geojson (tc/dataset "data/cb_2023_us_cbsa_500k.json"))

(def metropolitan-areas
  {17140 "Cincinnati, OH-KY-IN"
   17460 "Cleveland-Elyria, OH"
   19740 "Denver-Aurora-Lakewood, CO"
   28140 "Kansas City, MO-KS"
   32820 "Memphis, TN-MS-AR"
   33340 "Milwaukee-Waukesha-West Allis, WI"
   35380 "New Orleans-Metairie, LA"
   38300 "Pittsburgh, PA"
   38900 "Portland-Vancouver-Hillsboro, OR-WA"
   39580 "Raleigh, NC"
   12580 "Baltimore-Columbia-Towson, MD"
   13820 "Birmingham-Hoover, AL"
   29820 "Las Vegas-Henderson-Paradise, NV"
   33460 "Minneapolis-St. Paul-Bloomington, MN-WI"
   36420 "Oklahoma City, OK"
   40060 "Richmond, VA"
   40380 "Rochester, NY"
   41700 "San Antonio-New Braunfels, TX"
   41940 "San Jose-Sunnyvale-Santa Clara, CA"
   45300 "Tampa-St. Petersburg-Clearwater, FL"
   12060 "Atlanta-Sandy Springs-Roswell, GA"
   14460 "Boston-Cambridge-Newton, MA-NH"
   16980 "Chicago-Naperville-Elgin, IL-IN-WI"
   19100 "Dallas-Fort Worth-Arlington, TX"
   19820 "Detroit-Warren-Dearborn, MI"
   26420 "Houston-The Woodlands-Sugar Land, TX"
   31080 "Los Angeles-Long Beach-Anaheim, CA"
   33100 "Miami-Fort Lauderdale-West Palm Beach, FL"
   35620 "New York-Newark-Jersey City, NY-NJ-PA"
   37980 "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD"
   38060 "Phoenix-Mesa-Scottsdale, AZ"
   40140 "Riverside-San Bernardino-Ontario, CA"
   41860 "San Francisco-Oakland-Hayward, CA"
   42660 "Seattle-Tacoma-Bellevue, WA"
   47900 "Washington-Arlington-Alexandria, DC-VA-MD-WV"})



;; (defn get-data-by-area [area]
;;   (let [pat (re-pattern (str "(?i).*" (->> (str/split area #"\s+") (str/join ".*")) ".*"))
;;         code (some #(when (re-matches pat (val %)) (key %)) metropolitan-areas)
;;         idx (some #(when (= code (second %)) (first %))
;;                   (map-indexed vector (:name groups)))]
;;     (when idx
;;       (nth (:data groups) idx))))


(def unit-size-code-ranges
  (update-vals
   {1 [0 500]
    2 [500 749]
    3 [750 999]
    4 [1000 1499]
    5 [1500 1999]
    6 [2000 2499]
    7 [2500 2999]
    8 [3000 3999]
    9 [4000 4000]}
   #(first %)
   #_(+ (first %) (/ (- (second %) (first %)) 2))))


(defn- get-population+housing-units [area]
  (let [data (get data-by-area area)
        total-people (-> (tc/aggregate data #(reduce + (get % "NUMPEOPLE")))
                         :summary first)
        num-housing-units (tc/row-count data)
        relevant-housing-units
        (tc/rows
         (tc/select-rows data #(and (>= (get % "STORIES") 2)
                                    (= 1 (get % "PORCH"))
                                    (contains? unit-size-code-ranges (get % "UNITSIZE"))))
         :as-maps)
        avg-unit-size (-> (reduce #(+ %1 (get unit-size-code-ranges (get %2 "UNITSIZE")))
                                  0 relevant-housing-units)
                          (/ (count relevant-housing-units))
                          double)
        #_(-> (tc/aggregate
               relevant-housing-units
               #(reduce (fn [& args]
                          (prn args)
                          (apply + args))
                        (get % "UNITSIZE")))
              :summary first
              (/ num-housing-units) double)

        ;; Percent of people living in units with a deck and two or more stories
        ratio
        (/ (reduce #(+ %1 (get %2 "NUMPEOPLE")) 0 relevant-housing-units)
           #_(-> (tc/aggregate
                  relevant-housing-units
                  #(reduce + (get % "NUMPEOPLE")))
                 :summary first)
           total-people)
        area (get metropolitan-areas area)
        ;; Percent of relevant housing units in the given area
        housing-ratio (/ (count relevant-housing-units) #_(tc/row-count relevant-housing-units)
                         num-housing-units)]
    {:area area
     :num-people (long (* ratio
                          (-> metro-populations
                              (tc/select-rows #(= area (get % "Metro")))
                              (tc/rows :as-maps)
                              first (get "Total"))))
     :percent (double (* ratio 100))
     :avg-unit-size avg-unit-size
     :num-housing-units (long (* housing-ratio
                                 (-> metro-housing-units
                                     (tc/select-rows #(= area (get % "Metro")))
                                     (tc/rows :as-maps)
                                     first (get "Total"))))}))

(def metro-populations+housing
  (into {} (mapv #(vector (get metropolitan-areas %)
                          (get-population+housing-units %))
                 (keys data-by-area))))

(comment
  ;; Represented areas
  (mapv metropolitan-areas
        (:name (tc/group-by all-data "OMB13CBSA")))
  ;; Estimate of total number of people living in buildings with two stories or more and with a deck
  (let [data (mapv get-population+housing-units (keys data-by-area))
        total-people (reduce #(+ %1 (:num-people %2)) 0 data)
        avg-percent (/ (reduce #(+ %1 (:percent %2)) 0 data) (count data))]
    (prn total-people avg-percent)))


(clerk/table (vals metro-populations+housing))

(clerk/vl
 {:width 500
  :height 300
  ;; :data {:url "data/cb_2023_us_csa_500k_topo.json"
  ;;        :format {:type "topojson"
  ;;                 :feature "cb_2023_us_csa_500k"}}

  :projection {:type "albersUsa"}
  :mark "geoshape"})
