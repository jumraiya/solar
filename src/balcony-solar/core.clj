(ns balcony-solar.core
  (:require [nextjournal.clerk :as clerk]))


;(clerk/serve! {:browse true})

(clerk/serve! {:watch-paths ["src"]})

