(ns main
  (:require [nextjournal.clerk :as clerk]))

;; Start clerk server
(comment
  (clerk/serve! {:browse? true :watch-paths ["notebooks"]}))



(defn -main []
  (clerk/build! {:paths ["src/notebooks/*"]}))

(comment
  (-main))
