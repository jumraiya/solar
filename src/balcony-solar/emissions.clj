(ns balcony-solar.emissions 
  (:require
   [clojure.data.json :as json]
   [tablecloth.api :as tc]
   [nextjournal.clerk :as clerk]
   [balcony-solar.housing :as housing]
   [balcony-solar.insolation :as insolation]))

(def emissions-data
  (into {}
        (comp
         (map #(vector (get % "State") (dissoc % "State")))
         (map (fn [[k v]]
                [k (update-vals v
                         #(if (string? %)
                            (Double/parseDouble (.replace % "," ""))
                            %))])))
        (tc/rows
         (tc/dataset "data/grid_emissions_state.tsv")
         :as-maps)))


(def state-codes
  {"Alabama" "AL"
   "Alaska" "AK"
   "Arizona" "AZ"
   "Arkansas" "AR"
   "California" "CA"
   "Colorado" "CO"
   "Connecticut" "CT"
   "Delaware" "DE"
   "Florida" "FL"
   "Georgia" "GA"
   "Hawaii" "HI"
   "Idaho" "ID"
   "Illinois" "IL"
   "Indiana" "IN"
   "Iowa" "IA"
   "Kansas" "KS"
   "Kentucky" "KY"
   "Louisiana" "LA"
   "Maine" "ME"
   "Maryland" "MD"
   "Massachusetts" "MA"
   "Michigan" "MI"
   "Minnesota" "MN"
   "Mississippi" "MS"
   "Missouri" "MO"
   "Montana" "MT"
   "Nebraska" "NE"
   "Nevada" "NV"
   "New Hampshire" "NH"
   "New Jersey" "NJ"
   "New Mexico" "NM"
   "New York" "NY"
   "North Carolina" "NC"
   "North Dakota" "ND"
   "Ohio" "OH"
   "Oklahoma" "OK"
   "Oregon" "OR"
   "Pennsylvania" "PA"
   "Rhode Island" "RI"
   "South Carolina" "SC"
   "South Dakota" "SD"
   "Tennessee" "TN"
   "Texas" "TX"
   "Utah" "UT"
   "Vermont" "VT"
   "Virginia" "VA"
   "Washington" "WA"
   "West Virginia" "WV"
   "Wisconsin" "WI"
   "Wyoming" "WY"})

(def state-emissions-topo-json
  (-> "data/states-10m.json"
      slurp
      json/read-str
      (update-in ["objects" "states" "geometries"]
                 (fn [geometries]
                   (mapv
                    #(assoc-in % ["properties" "emissions"]
                               (get-in emissions-data
                                       [(get state-codes
                                             (get-in % ["properties" "name"]))
                                        "CO2"]))
                    geometries)))))

(clerk/vl
 {:width 700
  :height 600
  :data {:values state-emissions-topo-json
         :format {:type "topojson"
                  :feature "states"}}
  :projection {:type "albersUsa"}
  :mark "geoshape"
  :encoding {:color {:field "properties.emissions" :type "quantitative"}}})

(def emission-savings
  (reduce-kv
   (fn [savings state {:strs [CO2]}]
     (let [state (get (clojure.set/map-invert state-codes) state)
           areas (into []
                       (comp
                        (filter #(= (val %) state))
                        (map key))
                       insolation/metro-states)
           potential-power (long
                            (/
                             (reduce (fn [total area]
                                       (+ total
                                          (*
                                           (:num-housing-units
                                            (get housing/metro-populations+housing area))
                                           (first
                                            (get insolation/insolation-by-area area)))))
                                     0 areas)
                             1000))]
       (assoc savings state [(long (* 0.0004535924
                                      (* CO2 potential-power)))
                             potential-power])))
   {}
   emissions-data))


(clerk/table
 (clerk/use-headers
  (into [["State" "CO2 savings (tonnes)" "Power Generated (MWh)"]]
        (->> emission-savings
             (mapv flatten)
             
             (sort-by second)
             reverse))))
