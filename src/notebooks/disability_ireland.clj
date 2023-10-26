(ns notebooks.disability-ireland
  {:nextjournal.clerk/visibility {:code :fold}
   :nextjournal.clerk/toc true}
  (:require
   [aerial.hanami.common :as hc]
   [aerial.hanami.templates :as ht]
   [clojure.string :as str]
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]))


;; # Irish population with a disability 'to a great extent'
;;
;; This data is based on the recent Irish census (2022) which asked respondants about
;; whether they had a disability under three headings:
;; - Any disability
;; - A disability to some extent
;; - A disability to a great extent
;;
;; Here, I will focus only on the last of these areas (disabilty to a great extent).

;; Link to data source (Irish CSO website) - [CSO Census 2022 - Disability, Health and Carers](https://data.cso.ie/product/C2022P4)
;;
;;
;;
;; TODO Disability type (vision, hearing, ID, learning, physical, psychological, etc.)
;;
;; TODO Unemployment rate

;; TODO Housing


^{::clerk/visibility {:result :hide}}
(def file-persons-w-disability-type-f4005 "resources/data/disability/F4005.20231026T171007.csv")
^{::clerk/visibility {:result :hide}}
(def file-disability-age-sex-f4002 "resources/data/disability/dis_age_sex.csv")
^{::clerk/visibility {:result :hide}}
(def ireland-topo "resources/data/topo/ireland.topojson")

^{::clerk/visibility {:result :hide}}
(defn clean-keyword [keyword]
  (str/replace keyword #"[\"\" ]" ""))

^{::clerk/visibility {:result :hide}}
(defn translate-county-labels [label]
  (if (re-find #"Dublin|Rathdown|Fingal" label) "Dublin"
      (first (re-seq #"\w+" label))))

^{::clerk/visibility {:result :hide}}
(def DS_counties
  (-> file-persons-w-disability-type-f4005
      (tc/dataset {:key-fn (comp keyword clean-keyword)})
      (tc/map-columns :county [:AdministrativeCounties] #(translate-county-labels %))))

^{::clerk/visibility {:result :hide}}
(def DS_age_sex
  (-> file-disability-age-sex-f4002
      (tc/dataset {:key-fn (comp keyword clean-keyword)})))

^{::clerk/visibility {:result :hide}}
(defn transform-5-9
  "For displaying in graph (easier to order)"
  [ds]
  (-> ds
      (tc/map-columns :AgeGroup [:AgeGroup] #(if (= "5 - 9 years" %) "05 - 9 years" %))))

^{::clerk/visibility {:result :hide}}
(defn split-m-f-data [ds type m-f]
  (let [narrowed
        (-> ds
            (tc/select-rows (comp #(= type %) :UNIT))
            (transform-5-9)
            (tc/group-by :Sex)
            :data)
        sex (if (= m-f "male") (first narrowed) (second narrowed))]
    (-> sex
        (tc/group-by :AgeGroup)
        (tc/aggregate {:total #(reduce + (% :VALUE))})
        (tc/add-column :sex m-f))))


^{::clerk/visibility {:result :hide}}
(def mf-join
  (-> (split-m-f-data DS_age_sex "Number" "male")
      (tc/inner-join (split-m-f-data DS_age_sex "Number" "female") :$group-name)
      (tc/rename-columns {:$group-name :age-group
                          :total :total-male
                          :right.total :total-female})
      (tc/map-columns :age-group [:age-group] #(if (= % "85 years and over")
                                                 "85 +"
                                                 (str/replace % #" years" "")))))

;; TODO Try remove border here

;; ## Population with a Disability to a Great Extent
(clerk/vl
 (hc/xform
  ht/hconcat-chart
  :ALIGN "center"
  :HCONCAT [(hc/xform
             ht/bar-chart
             :DATA (-> mf-join (tc/rows :as-maps))
             :TITLE "Female"
             :X "total-female" :XTYPE "quantitative" :XSORT "descending" :XGRID false :XTITLE nil
             :Y "age-group" :YTYPE "nominal" :YAXIS nil :YSORT "descending"
             :MCOLOR "teal"
             :WIDTH 300
             :HEIGHT 350)
            { :$schema "https://vega.github.io/schema/vega-lite/v5.json",
             :description "A population pyramid for the US in 2000.",
             :data {:values (-> mf-join (tc/rows :as-maps))}
             :width 10
             :height 350
             :view {:stroke nil}
             :mark {:type "text"
                    :align "center"}
             :encoding {:y {:field "age-group"
                            :type "nominal"
                            :axis nil
                            :sort "descending"}
                        :text {:field "age-group"
                               :type "nominal"}}}
            (hc/xform
             ht/bar-chart
             :DATA (-> mf-join (tc/rows :as-maps))
             :TITLE "Male"
             :X "total-male" :XTYPE "quantitative" :XSCALE {:domain [0, 60000]} :XGRID false :XTITLE nil
             :Y "age-group" :YTYPE "nominal" :YSORT "descending" :YAXIS nil
             :WIDTH 300
             :MCOLOR "purple"
             :HEIGHT 350)]))

;; ## Map of the Data

^{::clerk/visibility {:result :hide}}
(def county-data-both-sexes
  (-> DS_counties
      (tc/select-rows (comp #(= "Both sexes" %) :Sex))
      (tc/group-by :county)            ; to get rid of multiple 'Dublin' entries
      (tc/aggregate {:people #(reduce + (% :VALUE))})
      (tc/rename-columns {:$group-name :county})
      (tc/select-columns [:county :people])
      (tc/drop-rows 0)))

^{::clerk/visibility {:result :hide}}
(def map-ireland (slurp ireland-topo))

(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :data {:format {:feature "counties" :type "topojson"}
         :values map-ireland}
  :height 450
  :width 600
  :title "People with a Disability to a Great Extent"
  :transform [{:lookup "id"
               :from {:data {:values (-> county-data-both-sexes
                                         (tc/rows :as-maps))}
                      :fields ["people"]
                      :key "county"}}]

  :mark "geoshape"
  :encoding {:color {:field "people" :type "quantitative"}}})

(clerk/table (->  county-data-both-sexes
                  (tc/order-by [:people] [:desc])))
