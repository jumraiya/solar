;; API functions for retrieving solar data from NSRDB
(ns balcony-solar.nsrdb
  (:require [clj-http.client :as client]))

(def API-KEY "woYD9w38NBW7hOEC3hIuwIZsQOoANfP2YBFHHqxr")

(defn get-data-for-area [area]
  (client/post "https://developer.nrel.gov/api/nsrdb/v2/solar/himawari-tmy-download"
               {:query-params {:api_key API-KEY}
                :form-params {:wkt "POINT(-122.083 37.423)"
                              :attributes "dni,dhi"
                              :names "tmy-2020"}}))
