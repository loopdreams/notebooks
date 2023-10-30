(ns main
  (:require [nextjournal.clerk :as clerk]
            [aerial.hanami.common :as hc]))

;; Hanami Defaults
^{::clerk/visibility {:result :hide}}
(swap! hc/_defaults assoc :BACKGROUND "white")


;; Start clerk server
(comment
  (clerk/serve! {:browse? true :watch-paths ["notebooks"]}))



(defn -main []
  (clerk/build! {:paths ["src/notebooks/*"]}))

(comment
  (-main))
