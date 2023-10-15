(ns main
  (:require [nextjournal.clerk :as clerk]))

;; Start clerk server
(comment
  (clerk/serve! {:browse? true :watch-paths ["src"]}))


(defn -main []
  (clerk/build! {:paths ["src/notebooks/*"]
                 :bundle true}))

(comment
  (-main))
