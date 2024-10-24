(ns balcony-solar.insolation
  (:require [tablecloth.api :as tc]
            [balcony-solar.housing :as housing]
            [nextjournal.clerk :as clerk]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.math :as math])
  (:import [java.time LocalDate]
           [java.lang Math]))

(def insolation-files
  {"Richmond, VA" "RichmondVA.csv"
   "Pittsburgh, PA" "PittsburghPA_40.45_-79.98_tmy-2022.csv"
   "Rochester, NY" "RochesterNY_43.17_-77.62_tmy-2022.csv"
   "Baltimore-Columbia-Towson, MD" "BaltimoreMD_39.29_-76.62_tmy-2022.csv"
   "Denver-Aurora-Lakewood, CO" "denver_tmy-2022.csv"
   "New Orleans-Metairie, LA" "NewOrleansLA_29.97_-90.06_tmy-2022.csv"
   "Memphis, TN-MS-AR" "MemphisTN_35.13_-90.06_tmy-2022.csv"
   "Cincinnati, OH-KY-IN" "cincinnati_tmy-2022.csv"
   "San Jose-Sunnyvale-Santa Clara, CA" "SanJoseCA_37.33_-121.90_tmy-2022.csv"
   "Cleveland-Elyria, OH" "cleveland_tmy-2022.csv"
   "Portland-Vancouver-Hillsboro, OR-WA" "PortlandOR_45.53_-122.66_tmy-2022.csv"
   "Kansas City, MO-KS" "kansas_city_tmy-2022.csv"
   "Oklahoma City, OK" "OklahomaCityOK_35.45_-97.50_tmy-2022.csv"
   "Raleigh, NC" "RaleighNC_35.77_-78.62_tmy-2022.csv"
   "San Antonio-New Braunfels, TX" "SanAntonioTX_29.41_-98.50_tmy-2022.csv"
   "Tampa-St. Petersburg-Clearwater, FL" "TampaFL_27.93_-82.46_tmy-2022.csv"
   "Milwaukee-Waukesha-West Allis, WI" "MilwaukeeWI_43.05_-87.90_tmy-2022.csv"
   "Minneapolis-St. Paul-Bloomington, MN-WI" "MinneapolisMN_44.97_-93.26_tmy-2022.csv"
   "Birmingham-Hoover, AL" "BirminghamAL_33.53_-86.82_tmy-2022.csv"
   "Las Vegas-Henderson-Paradise, NV" "LasVegasNV_36.17_-115.14_tmy-2022.csv"})

(def latitudes
  {"Richmond, VA" 37.53
   "Pittsburgh, PA" 40.45
   "Rochester, NY" 43.17
   "Baltimore-Columbia-Towson, MD" 39.29
   "Denver-Aurora-Lakewood, CO" 39.73
   "New Orleans-Metairie, LA" 29.97
   "Memphis, TN-MS-AR" 35.13
   "Cincinnati, OH-KY-IN" 39.09
   "San Jose-Sunnyvale-Santa Clara, CA" 37.33
   "Cleveland-Elyria, OH" 41.49
   "Portland-Vancouver-Hillsboro, OR-WA" 45.53
   "Kansas City, MO-KS" 39.09
   "Oklahoma City, OK" 35.45
   "Raleigh, NC" 35.77
   "San Antonio-New Braunfels, TX" 29.41
   "Tampa-St. Petersburg-Clearwater, FL" 27.93
   "Milwaukee-Waukesha-West Allis, WI" 43.05
   "Minneapolis-St. Paul-Bloomington, MN-WI" 44.97
   "Birmingham-Hoover, AL" 33.53
   "Las Vegas-Henderson-Paradise, NV" 36.17})

(def metro-states
  {"Richmond, VA" "Virginia"
   "Pittsburgh, PA" "Pennsylvania"
   "Rochester, NY" "New York"
   "Baltimore-Columbia-Towson, MD" "Maryland"
   "Denver-Aurora-Lakewood, CO" "Colorado"
   "New Orleans-Metairie, LA" "Louisiana"
   "Memphis, TN-MS-AR" "Tennessee"
   "Cincinnati, OH-KY-IN" "Ohio"
   "San Jose-Sunnyvale-Santa Clara, CA" "California"
   "Cleveland-Elyria, OH" "Ohio"
   "Portland-Vancouver-Hillsboro, OR-WA" "Oregon"
   "Kansas City, MO-KS" "Kansas"
   "Oklahoma City, OK" "Oklahoma"
   "Raleigh, NC" "North Carolina"
   "San Antonio-New Braunfels, TX" "Texas"
   "Tampa-St. Petersburg-Clearwater, FL" "Florida"
   "Milwaukee-Waukesha-West Allis, WI" "Wisconsin"
   "Minneapolis-St. Paul-Bloomington, MN-WI" "Minnesota"
   "Birmingham-Hoover, AL" "Alabama"
   "Las Vegas-Henderson-Paradise, NV" "Nevada"})

^{::clerk/visibility {:code :hide :result :hide}}
(def annual-electricity-prices
  (transduce
   (comp
    (filter #(str/includes? % "residential"))
    (map #(str/split % #","))
    (map #(vector
           (second (re-find #"(\w+)\s-*:\s-*residential" (first %)))
           (subvec % 3))))
   (completing
    (fn [data [state prices]]
      (assoc data state
             (zipmap
              ["Jul 2023" "Aug 2023" "Sep 2023" "Oct 2023" "Nov 2023" "Dec 2023" "Jan 2024" "Feb 2024" "Mar 2024" "Apr 2024" "May 2024" "Jun 2024" "Jul 2024"]
                (mapv parse-double prices)))))
   {}
   (-> "data/Average_retail_price_of_electricity.csv"
       (io/reader)
       line-seq)))


;; (defn- read-stream-line [stream]
;;   (loop [s (StringBuilder.)] 
;;     (let [c (.read stream)]
;;       (if (= c 10) 
;;         (.toString s)
;;         (recur (.append s (char c)))))))

(def SOLAR-PANEL-RATING 0.8) ;; 800W or 0.8 kW

(defn- calc-declination-angle [year month day]
  (let [day-of-year (.getDayOfYear (LocalDate/of year month day))]
    (* 23.45 (Math/sin (Math/toRadians (* (/ 360 365) (+ 284 day-of-year)))))))


#_(defn- calc-insolation-hour [latitude {:strs [Year Month Day DHI DNI]}]
  (let [declination (calc-declination-angle Year Month Day) 
        elevation-angle (+ (- 90 latitude) declination)]
    (+ DHI (* DNI (Math/sin (+ elevation-angle 90))))))

 #_(defn- calc-incident-angle-factor
   [latitude _azimuth-angle hour-of-day slope declination-angle]
   (let [hour-angle (Math/toRadians (- 180 (* hour-of-day 15)))]
     (+ (* (Math/sin declination-angle) (Math/sin (- latitude slope)))
        (* (Math/cos declination-angle) (Math/cos hour-angle) (Math/cos (- latitude slope))))))

(defn- calc-incident-angle-factor
    "Attempts to compute cosine of angle of sun's rays incident on the collector based on following inputs
   Latitude (latitude), 
   Surface azimuth angle (azimuth-angle): Angle bw surface normal and due south direction, 
   Hour of day (hour-of-day): Maps to angle bw 180 in morning to -180 at midnight with 15 degrees per hour,
   Surface slope (slope):  Angle of surface wrt to horizontal plane
   Declination angle (declination-angle): Angle made by line joining center of the sun and the earth w.r.t to equatorial plane (+23.45 o to -23.45o)"
    [latitude azimuth-angle hour-of-day slope declination-angle]
    (let [hour-angle (Math/toRadians (- 180 (* hour-of-day 15)))
          term-1 (* (Math/sin latitude)
                    (+ (* (Math/sin declination-angle) (Math/cos slope))
                       (* (Math/cos declination-angle) (Math/cos azimuth-angle) (Math/cos hour-angle) (Math/sin slope))))
          term-2 (* (Math/cos latitude)
                    (- (* (Math/cos declination-angle) (Math/cos hour-angle) (Math/cos slope))
                       (* (Math/sin declination-angle) (Math/cos azimuth-angle) (Math/sin slope))))
          term-3 (* (Math/cos declination-angle) (Math/sin azimuth-angle) (Math/sin hour-angle) (Math/sin slope))]
      (+ term-1 term-2 term-3)))

(defn- calc-insolation-hour [latitude {:strs [Year Month Day Hour DNI]}]
  (let [declination (calc-declination-angle Year Month Day)
        angle-factor (calc-incident-angle-factor
                      (Math/toRadians latitude)
                      0
                      Hour
                      (Math/toRadians 90)
                      (Math/toRadians declination))]
    (Math/abs (* DNI angle-factor))))


#_(defn- calc-avg-housing-surface-area [area]
  (let [{:keys [avg-unit-size num-housing-units]} (get housing/metro-populations+housing area)
        porch-area (* 0.5 (math/sqrt avg-unit-size))]
    (* porch-area num-housing-units)))

(defn- calc-yearly-power-generation [area]
  (let [ds (tc/dataset (str "data/insolation/" (get insolation-files area)))
        lat (get latitudes area)]
    (reduce
     (fn [[gen-total raw-total] data]
       [(+ gen-total
           ;; 20% solar panel efficiency with 70% conversion efficiency
           (min SOLAR-PANEL-RATING
                (* 0.2 0.7 (/ (calc-insolation-hour lat data) 1000))))
        (+ raw-total (get data "DNI") (get data "DHI"))])
     [0 0]
     (tc/rows ds :as-maps))))


(def insolation-by-area
  (into {}
        (map #(let [[gen-total raw-total] (calc-yearly-power-generation %)
                    avg-cost (long (/
                                    (reduce
                                     +
                                     (->> %
                                          (get metro-states)
                                          (get annual-electricity-prices)
                                          vals))
                                    12))]
                [% [(long gen-total) raw-total avg-cost
                    (long (/ (* gen-total avg-cost) 100))]]))
        (keys housing/metro-populations+housing)))

(clerk/col {:nextjournal.clerk/width :full}
           (clerk/table
            (clerk/use-headers
             (into [["Metro Area" "Avg Housing Unit Power Generation (kWH/year)" "Total insolation" "Avergage Electricity Cost (cents/kWH)" "Average Savings($)"]]
                   (mapv flatten
                         (reverse (sort-by (comp first val) insolation-by-area))))))
           (clerk/table [["Total potential power (MWH)"
                          (reduce
                           (fn [total [area [power]]]
                             (+ total
                                (long
                                 (/ (* (get-in housing/metro-populations+housing
                                               [area :num-housing-units])
                                       power)
                                    1000))))
                           0
                           insolation-by-area)
                          "Total potential savings ($)"
                          (reduce
                           (fn [total [area [_ _ _ savings]]]
                             (+ total
                                (* (get-in housing/metro-populations+housing
                                           [area :num-housing-units])
                                   savings)))
                           0
                           insolation-by-area)]]))



(clerk/vl
 {:width 700
  :height 600
  :data {:values (update-in housing/metro-areas-topo-json
                            ["objects" "cb_2023_us_csa_500k" "geometries"]
                            (fn [geometries]
                              (mapv
                               #(assoc-in % ["properties" "power"]
                                          (->> (get-in % ["properties" "NAME"])
                                               (get housing/csa-rev-map)
                                               (get insolation-by-area)
                                               first)
                                          #_(first
                                           (get insolation-by-area
                                                (get housing/csa-rev-map
                                                     (get-in % ["properties" "NAME"]))
                                                [0])))
                               geometries)))
         :format {:type "topojson"
                  :feature "cb_2023_us_csa_500k"}}
  :projection {:type "albersUsa"}
  :mark "geoshape"
  :encoding {:color {:field "properties.power" :type "quantitative"}}})


(clerk/vl
 {:width 700
  :height 600
  :data {:values (update-in housing/metro-areas-topo-json
                            ["objects" "cb_2023_us_csa_500k" "geometries"]
                            (fn [geometries]
                              (mapv
                               #(assoc-in % ["properties" "Avg_Electricity_Price"]
                                          (/
                                           (reduce
                                            +
                                            (->> (get-in % ["properties" "NAME"])
                                                 (get housing/csa-rev-map)
                                                 (get metro-states)
                                                 (get annual-electricity-prices)
                                                 vals))
                                           12))
                               geometries)))
         :format {:type "topojson"
                  :feature "cb_2023_us_csa_500k"}}
  :projection {:type "albersUsa"}
  :mark "geoshape"
  :encoding {:color {:field "properties.Avg_Electricity_Price" :type "quantitative"}}})
