(ns main
  (:require [nextjournal.clerk :as clerk]))

;; Start clerk server
(comment
  (clerk/serve! {:browse? true :watch-paths ["notebooks"]}))

(clerk/show! "src/notebooks/global_temp.clj")


(defn -main []
  (clerk/build! {:paths ["src/notebooks/*"]
                 :bundle true}))

(comment
  (-main))
