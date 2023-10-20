(ns index
  {:nextjournal.clerk/visibility {:code :hide}}
  (:require [nextjournal.clerk :as clerk]))

(clerk/html
 (into
  [:div.md:grid.md:ghap-8:grid-cols-2.pb8]
  (map
   (fn [{:keys [path title description]}]
     [:a.rounded-lg.shadow-lg.border.border-gray-300.relative.flex.flex-col.hover:border-indigo-600.group.mb-8.md:mb-0
      {:href (clerk/doc-url path) :title path :style {:height 75}}
      [:div.sans-serif.border-t.border-gray-300.px-4.py-2.group-hover:border-indigo-600
       [:div.font-bold.block.group-hover:text-indigo-600 title]
       [:div.text-xs.text-gray-500.group-hover:text-indigo-600.leading-normal description]]])
   [{:title "Most Streamed Spotify Songs 2023"
     :path "notebooks/spotify.clj"
     :description "Looking into some 2023 spotify stats using a data file from Kaggle"}])))
