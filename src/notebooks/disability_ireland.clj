(ns notebooks.disability-ireland
  {:nextjournal.clerk/visibility {:code :fold}
   :nextjournal.clerk/toc true}
  (:require
   [aerial.hanami.common :as hc]
   [aerial.hanami.templates :as ht]
   [clojure.string :as str]
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]))

^{::clerk/visibility {:result :hide}}
(swap! hc/_defaults assoc :BACKGROUND "white")

{::clerk/visibility {:result :hide}}
(def file-overview-disability-types-f4002 "resources/data/disability/disability_types_overiew_F4002.csv")
(def file-counties-persons-w-disability-type-f4005 "resources/data/disability/counties_disability_type_F4005.csv")
(def file-counties-all-population-f4042 "resources/data/disability/population_F4042.csv")
(def file-disability-age-sex-f4002 "resources/data/disability/dis_age_sex.csv")
(def file-dis-type-f4005 "resources/data/disability/dis_types_f4005.csv")
(def file-dis-type-age-f4006 "resources/data/disability/type_single_year_age_F4006.csv")
(def file-great-extent-SYOA-f4006 "resources/data/disability/great_extent_single_age_F4006.csv")
(def file-unemployment-people-f4058 "resources/data/disability/unemployment_people_F4058.csv")
(def file-unemployment-age-f4067 "resources/data/disability/unemployment_age_F4067.csv")
(def file-total-occupation-both-f4049 "resources/data/disability/total_occupation_both_F4049.csv")
(def file-total-occupation-sex-f4049 "resources/data/disability/total_occupation_sex_F4049.csv")
(def file-households-general-f4054 "resources/data/disability/households_any_F4054.csv")
(def file-households-type-f4054 "resources/data/disability/households_house_type_F4054.csv")
(def file-households-ID-f4054 "resources/data/disability/household_ID_housetype_F4054.csv")
(def file-households-mobility-f4054 "resources/data/disability/households_mobility_housetype_F4054.csv")
(def file-ireland-topo "resources/data/topo/ireland.topojson")

(def map-ireland (slurp file-ireland-topo))

;; # Irish population with a disability 'to a great extent'
;;
;; This data is based on the recent Irish census (2022) which grouped responses
;; regarding the question of disability under three headings:
;; - Any disability
;; - A disability to some extent
;; - A disability to a great extent
;;
;; Here, I will focus only on the last of these areas (**disabilty to a great extent**).

;; Link to data source (Irish CSO website) - [CSO Census 2022 - Disability, Health and Carers](https://data.cso.ie/product/C2022P4)
;;


(defn clean-keyword [keyword]
  (str/replace keyword #"[\"\" \p{C}]" ""))

(defn make-ds [file] (tc/dataset file {:key-fn (comp keyword clean-keyword)}))

(defn agestr->int [str]
  (if (re-find #"Under" str) 0
      (parse-long (re-find #"\d+" str))))


(def DS_age_sex (-> file-disability-age-sex-f4002 make-ds))
(def DS_age_sex_SYOA (-> file-great-extent-SYOA-f4006
                         make-ds
                         (tc/drop-rows (comp #(= "Both sexes" %) :Sex))))

(defn transform-5-9
  "For displaying in graph (easier to order)"
  [ds age-type]
  (-> ds
      (tc/map-columns age-type [age-type] #(if (= "5 - 9 years" %) "05 - 9 years" %))))

(defn split-m-f-data [ds value-type m-f age-type]
  (let [narrowed
        (-> ds
            (tc/select-rows (comp #(= value-type %) :UNIT))
            (transform-5-9 age-type)
            (tc/group-by :Sex)
            :data)
        sex (if (= m-f "male") (first narrowed) (second narrowed))]
    (-> sex
        (tc/group-by age-type)
        (tc/aggregate {:total #(reduce + (% :VALUE))})
        (tc/add-column :sex m-f))))

(defn mf-join-fn [ds age-type]
  (-> (split-m-f-data ds "Number" "male" age-type)
      (tc/inner-join (split-m-f-data ds "Number" "female" age-type) :$group-name)
      (tc/rename-columns {:$group-name :age-group
                          :total :total-male
                          :right.total :total-female})
      (tc/map-columns :age-group [:age-group] #(case age-type
                                                 :AgeGroup (if (= % "85 years and over")
                                                             "85 +"
                                                             (str/replace % #" years" ""))
                                                 :SingleYearofAge (if (= % "Under 1 year")
                                                                    0
                                                                    (agestr->int %))))))

(def mf-join (mf-join-fn DS_age_sex :AgeGroup))

(def mf-join-SYOA (mf-join-fn DS_age_sex_SYOA :SingleYearofAge))

(defn filter-overview-ds [unit]
  (-> file-overview-disability-types-f4002
      make-ds
      (tc/select-rows #(= unit (% :UNIT)))
      (tc/rows :as-maps)))


;; ## Context - Breif overview of Disability Levels
;; As mentioned at the outset, I will only look at the cagetory relating to
;; Disability 'to a great extent' here. For context, here is how 'disability to a great
;; extent' compared to the other categories. People with a disability make up **21.5%**
;; of the population, which is comprised of
;;  - (a) people with a disability to some extent (**13.6%** of the population)
;;  - (b) people with a disability to a great extent (**7.9%** of the population, or around 400,000 people).
;;
;; In other words, of peole with a disability in Ireland, **36.7%** were disabled to a great extent.
;;
;; Across all categories there is also a higher proportion of 'Female' than 'Male' who
;; are disabled. This is probably due to the difference in life expectancy between men
;; and women in Ireland, since age can also be linked to certain disability types. According
;; to [The Department of Health](https://www.gov.ie/en/publication/fdc2a-health-in-ireland-key-trends-2022/)
;; life expectancy for women was 84.4 years in 2020, while it was 80.8 for men. As can be seen
;; in the population pyramid below, the number of women disabled to a great extent over the age of
;; 85 (approx. 55,000) is the highest group across all age categories, male or female.

{::clerk/visibility {:result :show}}
(clerk/vl
 (hc/xform
  ht/grouped-bar-chart
  :TITLE "Disability Extent - Number of People"
  :DATA (filter-overview-ds "Number")
  :WIDTH 200
  :X "Sex" :XTYPE "Nominal"
  :Y "VALUE" :YTYPE "quantitative" :YTITLE "Number of People"
  :COLUMN :StatisticLabel :COLTYPE "nominal"))

(clerk/vl
 (hc/xform
  ht/grouped-bar-chart
  :TITLE "Disability Extent - % of Population"
  :DATA (filter-overview-ds "%")
  :WIDTH 250
  :X "Sex" :XTYPE "Nominal"
  :Y "VALUE" :YTYPE "quantitative" :YTITLE "% of Population"
  :COLUMN :StatisticLabel :COLTYPE "nominal"))


;; ## Population with a Disability to a Great Extent
;; Below are two population pyramids outlining the numbers of people disabled to a great
;; extent. The first chart shows ages grouped into 5 year bands, and the second shows 'single year
;; of age'. Most notably, there are three clear 'peaks' in the case of women (around ages 20, 60 and 85),
;; but only two for males (around ages 14/15 and 60-65).
;;
;; TODO Try remove border here
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
            {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
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


(clerk/vl
 (hc/xform
  ht/hconcat-chart
  :ALIGN "center"
  :HCONCAT [(hc/xform
             ht/bar-chart
             :DATA (-> mf-join-SYOA (tc/rows :as-maps))
             :TITLE "Female"
             :X "total-female" :XTYPE "quantitative" :XSORT "descending" :XGRID false :XTITLE nil
             :Y "age-group" :YTYPE "nominal" :YSORT "descending" :YAXIS nil
             :MCOLOR "teal"
             :WIDTH 300
             :HEIGHT 700)
            {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
             :data {:values (-> mf-join-SYOA (tc/rows :as-maps))}
             :width 10
             :height 700
             :view {:stroke nil}
             :mark {:type "text"
                    :align "center"}
             :encoding {:y {:field "age-group"
                            :type "quantitative"
                            :title "Years"}}}
            (hc/xform
             ht/bar-chart
             :DATA (-> mf-join-SYOA (tc/rows :as-maps))
             :TITLE "Male"
             :X "total-male" :XTYPE "quantitative" :XSCALE {:domain [0, 3500]} :XGRID false :XTITLE nil
             :Y "age-group" :YTYPE "nominal" :YSORT "descending" :YAXIS nil
             :WIDTH 300
             :MCOLOR "purple"
             :HEIGHT 700)]))

;; ## Disability to a Great Extent by County
;;
;; Topo JSON file for this map taken from this project - (https://andrewerrity.com/d3-project/).
;; Code available on [Github](https://github.com/aerrity/d3-cartogram).

{::clerk/visibility {:result :hide}}
(defn translate-county-labels [label]
  (if (re-find #"Dublin|Rathdown|Fingal" label) "Dublin"
      (first (re-seq #"\w+" label))))

(def DS_counties
  (-> file-counties-persons-w-disability-type-f4005 make-ds
      (tc/map-columns :county [:AdministrativeCounties] #(translate-county-labels %))))

(def county-data-both-sexes
  (-> DS_counties
      (tc/select-rows (comp #(= "Both sexes" %) :Sex))
      (tc/group-by :county)            
      (tc/aggregate {:people #(reduce + (% :VALUE))})
      (tc/rename-columns {:$group-name :county})
      (tc/select-columns [:county :people])
      (tc/drop-rows 0)))


(def all-population
  (-> file-counties-all-population-f4042
      make-ds
      (tc/map-columns :county [:AdministrativeCounties] #(translate-county-labels %))
      (tc/group-by :county)
      (tc/aggregate {:people #(reduce + (% :VALUE))})
      (tc/rename-columns {:$group-name :county
                          :people :population})))

(def population-join
  (-> all-population
      (tc/left-join county-data-both-sexes [:county])
      (tc/rename-columns {:people :disability-great-extent})
      (tc/select-columns [:county :population :disability-great-extent])
      (tc/map-columns :percentage-population [:population :disability-great-extent]
                      #(format "%.2f" (float (* 100 (/ %2 %1)))))))


{::clerk/visibility {:result :show}}
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


(clerk/table (->  population-join
                  (tc/map-columns :population [:population] #(format "%,12d" %))
                  (tc/map-columns :disability-great-extent [:disability-great-extent] #(format "%,12d" %))
                  (tc/order-by [:disability-great-extent] [:desc])))

;; We can see that Wexford has the highest level of people disabled to a great
;; extent as a proportion of the county's population,
;; while Meath has the lowest.

(clerk/vl
 (hc/xform
  ht/bar-chart
  :TITLE "Percentage Disability to a Great Extent by County"
  :DATA (-> population-join
            (tc/rows :as-maps))
  :X "percentage-population"
  :Y "county" :YTYPE "nominal" :YSORT "-x" :YTITLE ""
  :COLOR ht/default-color
  :CFIELD "disability-great-extent"
  :CTYPE "quantitative"
  :CSCALE {:scheme {:name "redpurple" :extent [0.1 1]}}
  :WIDTH 500
  :HEIGHT 500
  :TOOLTIP [{:field :X, :type :XTYPE, :title "%" , :format :XTFMT}
            {:field :Y, :type :YTYPE, :title "County" , :format :YTFMT}
            {:field "population" :type "quantitative" :title "Total Population"}
            {:field "disability-great-extent" :type "quantitative" :title "People with a Disability to a Great Extent"}]))


;; ## Disability Type

{::clerk/visibility {:result :hide}}
(def DS_type (-> file-dis-type-f4005 make-ds))

(defn aggregate-people-type [ds sex]
  (-> ds
      (tc/select-rows (comp #(= sex %) :Sex))
      (tc/group-by :StatisticLabel)
      (tc/aggregate {(keyword (str/replace sex #" " "")) #(reduce + (% :VALUE))})
      (tc/rename-columns {:$group-name :type})))

(def type-both-sexes (aggregate-people-type DS_type "Both sexes"))
(def type-male (aggregate-people-type DS_type "Male"))
(def type-female (aggregate-people-type DS_type "Female"))

(def aggregated-type-joined
  (-> type-both-sexes
      (tc/left-join type-male :type)
      (tc/left-join type-female :type)
      (tc/select-columns [:type :Bothsexes :Male :Female])))

(defn shorten-type-label [label]
  (case label
    "Blindness or vision impairment to greater extent"                                                                               "Vision"
    "Deafness or hearing impairment to greater extent"                                                                               "Hearing"
    "An intellectual disability to greater extent"                                                                                   "Intellectual Disability"
    "A difficulty with learning,remembering or concentrating to a greater extent"                                                    "Learning"
    "A difficulty with basic physical activities such as walking,  climbing stairs,reaching,lifting or carrying to a greater extent" "Physical Activities"
    "A difficulty with pain,breathing or any other chronic illness or condition to a greater extent"                                 "Chronic Illness"
    "A psychological or emotional condition or a mental health issue to a greater extent"                                            "Mental Health"
    "Dressing,bathing or getting around inside the home to a greater extent"                                                         "Home Mobility"
    "Working at a job or business or attending school or college to a greater extent"                                                "Working or attending school"
    "Participating in other activities,for example leisure or sport to a greater extent"                                             "Participation in Activities"
    "Going outside the home to shop or visit a doctor's surgery to a greater extent"                                                 "Outdoor Mobility"))

(def shortened-type
  (-> aggregated-type-joined
      (tc/map-columns :Difficulty_with [:type] #(shorten-type-label %))))


(def type-proportions
  (-> shortened-type
      (tc/map-columns :percentage-male [:Bothsexes :Male]
                      #(float (* 100 (/ %2 %1))))
      (tc/map-columns :percentage-female [:Bothsexes :Female]
                      #(float (* 100 (/ %2 %1))))
      (tc/map-columns :difference [:percentage-female :percentage-male]
                      #(- %1 %2))))

(defn type-proportions-chart [tbl]
  (let [ds (tc/rows tbl :as-maps)
        assoc-fn (fn [key name entry]
                   (-> {}
                       (assoc :difficulty (:Difficulty_with entry))
                       (assoc :value (key entry))
                       (assoc key (key entry))
                       (assoc :type name)))]
    (reduce (fn [result entry]
              (conj result
                    (assoc-fn :percentage-female "Female" entry)
                    (assoc-fn :percentage-male "Male" entry)))
            []
            ds)))


{::clerk/visibility {:result :show}}
(clerk/table
 (-> aggregated-type-joined (tc/order-by [:Bothsexes] [:desc])))

(clerk/vl
 (hc/xform
  ht/hconcat-chart
  :HCONCAT
  [(hc/xform
    ht/bar-chart
    :DATA (-> shortened-type (tc/rows :as-maps))
    :TITLE "Female"
    :X "Female" :XTYPE "quantitative"
    :XTITLE nil :XSORT "descending"
    :Y "Difficulty_with" :YTYPE "nominal" :YAXIS nil :YSORT "-x"
    :WIDTH 300
    :HEIGHT 400
    :COLOR "Difficulty_with"
    :TOOLTIP [{:field :X :type :XTYPE}
              {:field :Y :type :YTYPE :title "Dificulty shorthand"}
              {:field "type" :title "Dificulty Full Name"}])
   (hc/xform
    ht/bar-chart
    :DATA (-> shortened-type (tc/rows :as-maps))
    :TITLE "Male"
    :X "Male" :XTYPE "quantitative" :XTITLE nil
    :XSCALE {:domain [0, 200000]}
    :Y "Difficulty_with" :YTYPE "nominal" :YAXIS nil :YSORT "-x"
    :WIDTH 300
    :HEIGHT 400
    :COLOR "Difficulty_with"
    :LTITLE "Hello"
    :TOOLTIP [{:field :X :type :XTYPE}
              {:field :Y :type :YTYPE :title "Dificulty shorthand"}
              {:field "type" :title "Dificulty Full Name"}])]))

(clerk/vl
 (hc/xform
  ht/layer-chart
  :LAYER
  [(hc/xform
    ht/bar-chart
    :DATA (type-proportions-chart type-proportions)
    :X "difficulty" :XTYPE "nominal" :XSORT {:op "sum" :field "percentage-female"}
    :Y "value" :YTYPE "quantitative" :YTITLE "Percentage"
    :COLOR ht/default-color :CFIELD "type" :CSCALE {:scheme "paired"}
    :WIDTH 500)
   {:data {:values (type-proportions-chart type-proportions)}
    :mark {:type "line"  :color "firebrick" :strokeWidth 2}
    :encoding {:y {:field "value" :aggregate "average"}
               :x {:field "difficulty" :type "nominal" :sort {:op "sum" :field "percentage-female"}}}}]))

;; From the chart above, it seems that, except for Intellectual Disability and learning/remembering related disabilities,
;; there is a higher proportion on average of women who are disabled to a great extent.
;;
;; However, we also saw from the original population pyramid above that there are a very high number
;; of women (approx 55,000) who are over 85 and are disabled to a great extent. This is what might be expected,
;; given that life expectancy tends to be higher for women. So, to try account for this slight skew in the over 85 age
;; group, let's try to look at the same charts again, but with people aged over 85 removed.

;; ### Disability Type, under 85

{::clerk/visibility {:result :hide}}

(defn DS_type_age_restricted [age]
  (-> file-dis-type-age-f4006
      make-ds
      (tc/map-columns :age [:SingleYearofAge] #(agestr->int %))
      (tc/drop-rows (comp #(> % age) :age))))

(def age-restricted-male-84 (aggregate-people-type (DS_type_age_restricted 84) "Male"))
(def age-restricted-female-84 (aggregate-people-type (DS_type_age_restricted 84) "Female"))

(defn age-restricted-join [male_DS female_DS]
  (-> female_DS
      (tc/left-join male_DS :type)
      (tc/drop-columns [:right.type])
      (tc/map-columns :Difficulty_with [:type] #(shorten-type-label %))
      (tc/map-columns :total [:Male :Female] #(+ %1 %2))
      (tc/map-columns :percentage-male [:total :Male]
                      #(float (* 100 (/ %2 %1))))
      (tc/map-columns :percentage-female [:total :Female]
                      #(float (* 100 (/ %2 %1))))))

(def age-restricted-joined-84 (age-restricted-join age-restricted-male-84 age-restricted-female-84))
(def age-restricted-female-18 (aggregate-people-type (DS_type_age_restricted 18) "Female"))
(def age-restricted-male-18 (aggregate-people-type (DS_type_age_restricted 18) "Male"))
(def age-restricted-joined-18 (age-restricted-join age-restricted-male-18 age-restricted-female-18))

{::clerk/visibility {:result :show}}

(clerk/vl
 (hc/xform
  ht/hconcat-chart
  :HCONCAT
  [
   (hc/xform
    ht/layer-chart
    :LAYER
    [(hc/xform
      ht/bar-chart
      :TITLE "Under 65"
      :DATA (type-proportions-chart age-restricted-joined-84)
      :X "difficulty" :XTYPE "nominal" :XSORT {:op "sum" :field "percentage-female"}
      :Y "value" :YTYPE "quantitative" :YTITLE "Percentage"
      :COLOR ht/default-color :CFIELD "type" :CSCALE {:scheme "paired"}
      :WIDTH 250)
     {:data {:values (type-proportions-chart age-restricted-joined-84)}
      :mark {:type "line"  :color "firebrick" :strokeWidth 2}
      :encoding {:y {:field "value" :aggregate "average"}
                 :x {:field "difficulty" :type "nominal" :sort {:op "sum" :field "percentage-female"}}}}])
   (hc/xform
    ht/layer-chart
    :LAYER
    [(hc/xform
      ht/bar-chart
      :TITLE "Under 18"
      :DATA (type-proportions-chart age-restricted-joined-18)
      :X "difficulty" :XTYPE "nominal" :XSORT {:op "sum" :field "percentage-female"}
      :Y "value" :YTYPE "quantitative" :YAXIS nil :YSCALE {:domain [0 100]}
      :COLOR ht/default-color :CFIELD "type" :CSCALE {:scheme "paired"}
      :WIDTH 250)
     {:data {:values (type-proportions-chart age-restricted-joined-18)}
      :mark {:type "line"  :color "firebrick" :strokeWidth 2}
      :encoding {:y {:field "value" :aggregate "average"}
                 :x {:field "difficulty" :type "nominal" :sort {:op "sum" :field "percentage-female"}}}}])]))




 


;; ## Employment
{::clerk/visibility {:result :hide}}
(def DS_unemployment_sex
  (-> file-unemployment-age-f4067
      make-ds
      (tc/drop-rows (comp #(= "Both sexes" %) :Sex))))

(def DS_unemployment_both
  (-> file-unemployment-age-f4067
      make-ds
      (tc/drop-rows (comp #(= "Male" %) :Sex))
      (tc/drop-rows (comp #(= "Female" %) :Sex))))

(defn shorten-statistic-label [label]
  (case label
    "Population"                                                           "Population"
    "Population with any disability"                                       "Any Disability"
    "Population with a disability to a great extent"                       "Great Extent"
    "Population with a disability to some extent"                          "Some Extent"
    "Umemployment rate for total population"                               "Population Rate"
    "Unemployment rate for population with a disability"                   "Disability Rate"
    "Unemployment rate for population with a disability to a great extent" "Great Extent Rate"
    "Unemployment rate for population with a disability to a some extent"  "Some Extent Rate"))

(def unemployment-maps-sex
  (let [all-ds (map #(tc/rows % :as-maps)
                    (-> DS_unemployment_sex
                        (tc/drop-rows (comp #(= "15 years and over" %) :AgeGroup))
                        (tc/map-columns :StatisticLabel [:StatisticLabel] #(shorten-statistic-label %))
                        (tc/group-by :StatisticLabel)
                        :data))
        number-ds (reduce concat (take 3 (rest all-ds)))
        number-pop (first all-ds)
        rate-ds (reduce concat (drop 4 all-ds))]
    [number-ds number-pop rate-ds]))

(def unemployment-maps-both
  (let [all-ds (map #(tc/rows % :as-maps)
                    (-> DS_unemployment_both
                        (tc/drop-rows (comp #(= "15 years and over" %) :AgeGroup))
                        (tc/map-columns :StatisticLabel [:StatisticLabel] #(shorten-statistic-label %))
                        (tc/group-by :StatisticLabel)
                        :data))
        number-ds (reduce concat (take 3 (rest all-ds)))
        number-pop (first all-ds)
        rate-ds (reduce concat (drop 4 all-ds))]
    [number-ds number-pop rate-ds]))

(defn occupations  [file]
  (-> file
      make-ds
      (tc/drop-rows #(= "All occupational groups" (% :IntermediateOccupationalGroup)))
      (tc/rows :as-maps)))

(defn diff-two-rows [rows]
  (apply - (reverse (sort rows))))

;; ### Employment Types
;;
;; Here are all the occupational groupes included in the census:

{::clerk/visibility {:result :show}}
(clerk/md
 (str/join
  "\n"
  (rest
   (for [occupation (:IntermediateOccupationalGroup
                     (->  file-total-occupation-both-f4049 make-ds))]
     (str "* " occupation)))))


;; Here are the numbers of people disabled to a great extent employed in each of the occupational groups.
;; We can see that, outside of 'other occupation', the highest emplyment type is **Elementary administration
;; and service occupations**.



(clerk/vl
 (hc/xform
  ht/bar-chart
  :TITLE "Employment Types for Disability Great Extent"
  :DATA (occupations file-total-occupation-both-f4049)
  :Y "IntermediateOccupationalGroup" :YTYPE "nominal" :YSORT "-x" :YTITLE nil
  :X "VALUE" :XTPYE "quantitative"
  :HEIGHT 600
  :WIDTH 500))

;; Here is the same data as above split by sex:

(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :title "Employment Types for Disability Great Extent by Sex"
  :data {:values (occupations file-total-occupation-sex-f4049)}
  :mark {:type "bar" :tooltip true}
  :hight 400
  :width 480
  :encoding {:x {:field "VALUE" :type "quantitative"}
             :y {:field "IntermediateOccupationalGroup" :type "nominal" :sort "-x" :title ""}
             :yOffset {:field "Sex"}
             :color {:field "Sex"
                     :scale {:scheme "pastel1"}}}})

;; ### Unemployment

;; #### Unemployment Rate People disabiled to a great extent

(clerk/table
 (-> file-unemployment-people-f4058
     make-ds
     (tc/select-columns [:StatisticLabel :Sex :UNIT :VALUE])))

(clerk/vl
 (hc/xform
  ht/grouped-bar-chart
  :TITLE "Unemployment Rate A"
  :DATA (last unemployment-maps-both)
  :X "AgeGroup" :XTYPE "nominal" :XAXIS nil
  :Y "VALUE" :YTITLE "%"
  :WIDTH 100
  :COLOR ht/default-color :CFIELD "AgeGroup"
  :COLUMN "StatisticLabel" :COLTYPE "nominal"))

(clerk/vl
 (hc/xform
  ht/line-chart
  :TITLE "Unemployment Rate B"
  :DATA (remove #(or (= "15 - 19 years" (:AgeGroup %))
                     (= "65 years and over" (:AgeGroup %)))
                (last unemployment-maps-both))
  :X "AgeGroup" :XTYPE "nominal"
  :Y "VALUE" :YTITLE "%"
  :MSIZE 3
  :WIDTH 500
  :COLOR "StatisticLabel"))

(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :title "Unemployment Rate for Disability to a Great Extent by Sex"
  :data {:values (filter #(= "Great Extent Rate" (:StatisticLabel %))
                         (remove #(or (= "15 - 19 years" (:AgeGroup %))
                                      (= "65 years and over" (:AgeGroup %)))
                                 (last unemployment-maps-sex)))}
  :mark "bar"
  :encoding {:x {:field "AgeGroup"}
             :y {:field "VALUE" :type "quantitative"}
             :xOffset {:field "Sex"}
             :color {:field "Sex"
                     :scale {:scheme "paired"}}}})

;; #### Unemployment Rate Gap


;; This chart shows the distance between the unemployment rate for the general population and
;; that for those disabled to a great extent. As can be seen, the unemployment rate for those
;; disabled to a great extent is 12-16 percentage points above the general population.

(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA

  (-> DS_unemployment_both
      (tc/select-rows #(or (= "Unemployment rate for population with a disability to a great extent"
                              (% :StatisticLabel))
                           (= "Umemployment rate for total population"
                              (% :StatisticLabel))))
      (tc/drop-rows (comp #(= "65 years and over" %) :AgeGroup))
      (tc/drop-rows (comp #(= "15 - 19 years" %) :AgeGroup))
      (tc/group-by :AgeGroup)
      (tc/aggregate {:difference #(diff-two-rows (% :VALUE))})
      (tc/rename-columns {:$group-name :AgeGroup})
      (tc/rows :as-maps))

  :X "AgeGroup" :XTYPE "nominal"
  :Y "difference" :YTYPE "quantitative" :YTITLE "Difference %"
  :WIDTH 600))

;; ## Private Households

(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :title "Number in Private Households"
  :data {:values (-> file-households-general-f4054
                     make-ds
                     (tc/drop-rows #(= "Both sexes" (% :Sex)))
                     (tc/rows :as-maps))}
  :mark {:type "bar" :tooltip true}
  :width 500
  :height 400
  :encoding {:x {:field :Extentofdifficultyorcondition :type "nominal"}
             :y {:field "VALUE" :type "quantitative"}
             :xOffset {:field "Sex"}
             :color {:field "Sex"
                     :scale {:scheme "paired"}}}})

(clerk/table
 (-> file-households-general-f4054
     make-ds
     (tc/select-rows #(= "Both sexes" (% :Sex)))
     (tc/select-columns [:StatisticLabel
                         :Extentofdifficultyorcondition
                         :VALUE])))

;; As can be seen here, the total number of people disabled to a great extent in private households
;; is **366,323**, which is 41,110 people less than the total number disabled to a
;; great extent (407,342). I'm not sure about what this difference means. I've tried looking
;; through some of the census material for an explanation of the category 'private households' but I couldn't
;; find it. Does this imply that around forty thousand people are living in 'public' households? (i.e., residential care,
;; nursing homes, hospitals, etc.?) There is a similar difference between the totals for 'some extent' in private
;; households and total 'some extent' population.

;; ### Types of households

(clerk/vl
 (hc/xform
  ht/grouped-bar-chart
  :DATA
  (-> file-households-type-f4054
      make-ds
      (tc/select-rows #(= "Both sexes" (% :Sex)))
      (tc/rows :as-maps))
  :WIDTH 250
  :X :TypeofHousehold :XTYPE "nominal" :XSORT "-y"
  :Y "VALUE"
  :COLUMN :Extentofdifficultyorcondition :COLTYPE "nominal"))

(clerk/table
 (-> file-households-type-f4054
     make-ds
     (tc/select-rows #(= "Great Extent" (% :Extentofdifficultyorcondition)))
     (tc/select-rows #(= "Both sexes" (% :Sex)))
     (tc/select-columns [:Extentofdifficultyorcondition
                         :TypeofHousehold
                         :VALUE])
     (tc/order-by [:VALUE] [:desc])))

(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :title "Disability to a Great Extent Household Type/Sex"
  :data {:values (-> file-households-type-f4054
                     make-ds
                     (tc/select-rows #(= "Great Extent" (% :Extentofdifficultyorcondition)))
                     (tc/drop-rows #(= "Both sexes" (% :Sex)))
                     (tc/rows :as-maps))}
  :mark "bar"
  :width 500
  :height 400
  :encoding {:x {:field :TypeofHousehold :type "nominal" :sort "-y"}
             :y {:field "VALUE" :type "quantitative"}
             :xOffset {:field "Sex"}
             :color {:field "Sex"
                     :scale {:scheme "paired"}}}})


;; ### Intellectual Disability Housing Type



(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :title "Intellectual Disability to a Great Extent Household Type/Sex"
  :data {:values (-> file-households-ID-f4054
                     make-ds
                     (tc/select-rows #(= "Great Extent" (% :Extentofdifficultyorcondition)))
                     (tc/drop-rows #(= "Both sexes" (% :Sex)))
                     (tc/rows :as-maps))}
  :mark "bar"
  :width 500
  :height 400
  :encoding {:x {:field :TypeofHousehold :type "nominal" :sort "-y"}
             :y {:field "VALUE" :type "quantitative"}
             :xOffset {:field "Sex"}
             :color {:field "Sex"
                     :scale {:scheme "paired"}}}})

;; ### Difficulty with Dressing, bathing or getting around inside the home to a greater extent

(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :title "Home Mobility Difficulty to a Great Extent Household Type/Sex"
  :data {:values (-> file-households-mobility-f4054
                     make-ds
                     (tc/select-rows #(= "Great Extent" (% :Extentofdifficultyorcondition)))
                     (tc/drop-rows #(= "Both sexes" (% :Sex)))
                     (tc/rows :as-maps))}
  :mark "bar"
  :width 500
  :height 400
  :encoding {:x {:field :TypeofHousehold :type "nominal" :sort "-y"}
             :y {:field "VALUE" :type "quantitative"}
             :xOffset {:field "Sex"}
             :color {:field "Sex"
                     :scale {:scheme "paired"}}}})
