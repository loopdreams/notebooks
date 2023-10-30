(ns notebooks.global-temp
  {:nextjournal.clerk/visibility {:code :fold}
   :nextjournal.clerk/toc true}
  (:require
   [aerial.hanami.common :as hc]
   [aerial.hanami.templates :as ht]
   [clojure.string :as str]
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]
   [java-time.api :as jt]))

{::clerk/visibility {:result :hide}}
(swap! hc/_defaults assoc :BACKGROUND "white")

(def global_temperatures_file "resources/data/global_temperatures/GlobalTemperatures.csv")
(def country_temperatures_file "resources/data/global_temperatures/GlobalLandTemperaturesByCountry.csv")

(def DS (-> global_temperatures_file
            (tc/dataset {:key-fn keyword})))

(def DS_country (-> country_temperatures_file
                    (tc/dataset {:key-fn keyword})))

;; # Global Temperatures
;; - Source: [Global Temperatures Dataset on Kaggle](https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data)
;;
;; A dataset containing information about global temperatures from 1750 to 2015. It is a bit out of date now,
;; but I'm exploring it as a learning exercise.
;;
;; There are also complementary files containing the same info by Country, City, etc.


;; ## Dataset Info
;; The main dataset has 3192 rows and 9 columns.
{::clerk/visibility {:result :show}}

(clerk/table (-> DS (tc/info :basic) (tc/rows :as-maps)))

;; The columns are:

(clerk/md
 (str/join "\n"
           (for [i (-> DS (tc/info :columns) :name)]
             (str "* " (name i)))))

;; Looking at the summary info, we can see that the start data ranges from the **1st January, 1750**
;; until the **1st Decemeber, 2015**.

(clerk/table (-> DS tc/info))

;; ## Land Average Temperature
;; Let's start by just looking at the average temperature over the range.
;;
{::clerk/visibility {:result :hide}}
(def DS_A (-> DS (tc/map-columns :date [:dt] #(str %))))

(def winter (map jt/month [:december :january :february]))
(def spring (map jt/month [:march :april :may]))
(def summer (map jt/month [:june :july :august]))
(def autumn (map jt/month [:september :october :november]))

(defn assign-season [timestamp]
  (cond
    (some #{(jt/month timestamp)} winter) "winter"
    (some #{(jt/month timestamp)} spring) "spring"
    (some #{(jt/month timestamp)} summer) "summer"
    (some #{(jt/month timestamp)} autumn) "autumn"
    :else nil))

(def DS_season
  (-> DS_A
      (tc/map-columns :season [:dt] #(assign-season %))))

{::clerk/visibility {:result :show}}

(clerk/vl
 (hc/xform
  ht/point-chart
  :DATA (-> DS_A (tc/rows :as-maps))
  :X "date" :XTYPE "temporal"
  :Y "LandAverageTemperature"
  :WIDTH 600))


;; Interesting graph! Interesting to see an average differential
;; between June/July/August and December/January/February of around 10 degrees.
;; Let's try include that 'seasonal' information in the chart.


(clerk/vl
 (hc/xform
  ht/point-chart
  :DATA (-> DS_season (tc/rows :as-maps))
  :X "date" :XTYPE "temporal"
  :Y "LandAverageTemperature"
  :COLOR "season"
  :WIDTH 600))

;; ## Yearly Averages
^{::clerk/visibility {:result :hide}}
(def DS_averages
  (-> DS
      (tc/drop-missing :LandAverageTemperature)
      (tc/drop-missing :LandMaxTemperature)
      (tc/map-columns :year [:dt] #(jt/year %))
      (tc/map-columns :year [:year] #(str %))
      (tc/group-by :year)
      (tc/aggregate {:annual_avg
                     #(float (/
                              (reduce + (% :LandAverageTemperature))
                              (count (% :LandAverageTemperature))))
                     :annual_avg_incl_ocean
                     #(float (/
                              (reduce + (% :LandAndOceanAverageTemperature))
                              (count (% :LandAndOceanAverageTemperature))))
                     :max_avg_annual
                     #(float (/
                              (reduce + (% :LandMaxTemperature))
                              (count (% :LandMaxTemperature))))
                     :min_avg_annual
                     #(float (/
                              (reduce + (% :LandMinTemperature))
                              (count (% :LandMinTemperature))))})
      (tc/rename-columns {:$group-name :year})))

(clerk/vl
 (hc/xform
  ht/layer-chart
  :LAYER
  [(hc/xform
    ht/line-chart
    :DATA (-> DS_averages (tc/rows :as-maps))
    :X "year" :XTYPE "temporal"
    :Y :annual_avg_incl_ocean
    :MSIZE 5
    :MCOLOR "purple"
    :WIDTH 600)
   (hc/xform
    ht/line-chart
    :DATA (-> DS_averages (tc/rows :as-maps))
    :X "year" :XTYPE "temporal"
    :Y "annual_avg"
    :MCOLOR "green"
    :WIDTH 600)
   (hc/xform
    ht/line-chart
    :DATA (-> DS_averages (tc/rows :as-maps))
    :X "year" :XTYPE "temporal"
    :Y "max_avg_annual"
    :MCOLOR "firebrick")
   (hc/xform
    ht/line-chart
    :DATA (-> DS_averages (tc/rows :as-maps))
    :X "year" :XTYPE "temporal"
    :Y "min_avg_annual")]))

;; As we can see, over the period of about 160 years, the average for the min, average, and max annual land temperatures
;; rose by around 2 degrees celsius. The average for the land and ocean temperatures combined (purple line) rose by
;; around 1 degree celsius.

;; ## Highest Max Temperatures
;; Let's look at the highest temperatures on record.

(clerk/table
 (-> DS_season
     (tc/order-by [:LandMaxTemperature] [:desc])
     (tc/select-rows (range 11))
     (tc/select-columns [:date :LandMaxTemperature])))

;; Nine out of ten of the top days were in July and the oldest year was 1998.
;;

;; ### Higest Global Max and Lowest Min Temps for each year

{::clerk/visibility {:result :hide}}
(def DS_grouped_annual
  (-> DS_season
      (tc/map-columns :year [:dt] #(jt/year %))
      (tc/map-columns :month [:dt] #(jt/month %))
      (tc/drop-missing :LandMaxTemperature)
      (tc/map-columns :year [:year] #(str %))
      (tc/map-columns :month [:month] #(str %))
      (tc/group-by :year)))

(def yearly-max
  (-> DS_grouped_annual
      (tc/order-by [:LandMaxTemperature] [:desc])
      (tc/aggregate {:yearly-max #(first (% :LandMaxTemperature))
                     :max-month #(first (% :month))})))

(def yearly-min
  (-> DS_grouped_annual
      (tc/order-by [:LandMinTemperature])
      (tc/aggregate {:yearly-min #(first (% :LandMinTemperature))
                     :min-month #(first (% :month))})))

(def max-min-join
  (-> (tc/inner-join yearly-max yearly-min :$group-name)
      (tc/rename-columns {:$group-name :year})
      (tc/map-columns :min-max-gap [:yearly-max :yearly-min] #(- %1 %2))
      (tc/rows :as-maps)))

{::clerk/visibility {:result :show}}
(clerk/vl
 (hc/xform
  ht/layer-chart
  :LAYER
  [(hc/xform
    ht/point-chart
    :DATA max-min-join
    :X :year :XTYPE "temporal"
    :Y :yearly-max
    :COLOR "max-month"
    :WIDTH 600)
   (hc/xform
    ht/point-chart
    :DATA max-min-join
    :X :year :XTYPE "temporal"
    :Y :yearly-min
    :COLOR "min-month")
   (hc/xform
    ht/line-chart
    :DATA max-min-join
    :X :year :XTYPE "temporal"
    :Y :min-max-gap)]))

;; These 'extremes' (highest max global and lowest min global for the year) are increasing at a lower
;; rate than the averages. The line at the top also shows that the 'gap' between the two decreased, meaning that
;; the min temperatures incrased quicker than the max.

;; ### Land Averages by Month
{::clerk/visibility {:result :hide}}
(defn annual-avg-month [ds month]
  (-> ds
      (tc/map-columns :month [:dt] #(jt/month %))
      (tc/map-columns :year [:dt] #(str (jt/year %)))
      (tc/select-rows (comp #(= (str %) month) :month))
      (tc/map-columns :month [:month] #(str/capitalize (str %)))
      (tc/select-columns [:date :month :LandAverageTemperature :year])
      (tc/rows :as-maps)))

(def months ["January" "February" "March" "April" "May" "June" "July" "August" "September" "October" "November" "December"])

(def monthly-DS
  (reduce concat
          (map #(annual-avg-month DS_A %) (map str/upper-case months))))
  
{::clerk/visibility {:result :show}}
(clerk/vl
 (hc/xform
  ht/line-chart
  :DATA monthly-DS
  :X "date" :XTYPE "temporal"
  :Y "LandAverageTemperature"
  :COLOR ht/default-color :CFIELD "month" :CTYPE "nominal"
  :CSCALE {:domain months :range ["#cdddff" "#cdddff" 
                                  "#DAD4B5" "#DAD4B5" "#DAD4B5"
                                  "#FF6969" "#FF6969" "#FF6969"
                                  "#EA906C" "#EA906C" "#EA906C"
                                  "#cdddff"]}
  :WIDTH 600))

(clerk/vl
 (hc/xform
  ht/line-chart
  :DATA monthly-DS
  :X "month" :XTYPE "nominal" :XSORT months
  :Y "LandAverageTemperature"
  :COLOR ht/default-color :CFIELD "year" :CTYPE "temporal"
  :CSCALE {:scheme {:name "goldred" :extent [0.1 1.5]}}
  :WIDTH 600))

;; Taking only the years since 1820 (blue line is most recent year, 2015):

{::clerk/visibility {:result :hide}}
(defn annual-avg-month-1820 [ds month]
  (-> ds
      (tc/map-columns :month [:dt] #(jt/month %))
      (tc/map-columns :year [:dt] #(str (jt/year %)))
      (tc/select-rows (comp #(< 1820 (parse-long %)) :year))
      (tc/select-rows (comp #(= (str %) month) :month))
      (tc/map-columns :month [:month] #(str/capitalize (str %)))
      (tc/select-columns [:date :month :LandAverageTemperature :year])
      (tc/rows :as-maps)))

(def monthly-DS-1820
  (reduce concat
          (map #(annual-avg-month-1820 DS_A %) (map str/upper-case months))))

(def monthly-DS-1820-2014
  (remove #(= (:year %) "2015") monthly-DS-1820))

(def monthly-DS-2015
  (filter #(= (:year %) "2015") monthly-DS-1820))

{::clerk/visibility {:result :show}}
(clerk/vl
 (hc/xform
  ht/layer-chart
  :LAYER
  [(hc/xform
    ht/line-chart
    :DATA monthly-DS-1820-2014
    :X "month" :XTYPE "nominal" :XSORT months
    :Y "LandAverageTemperature"
    :OPACITY 0.6
    :COLOR ht/default-color :CFIELD "year" :CTYPE "temporal"
    :CSCALE {:scheme {:name "bluegreen" :extent [0.1 1]}}
    :POINT true
    :WIDTH 600)
   (hc/xform
    ht/line-chart
    :DATA monthly-DS-2015
    :X "month" :XTYPE "nominal" :XSORT months
    :Y "LandAverageTemperature"
    :MCOLOR "blue"
    :MSIZE 3
    :POINT true
    :WIDTH 600)]))

;; ## Country Data
;; In order to get the map below to work, I had to update a few of the country names to match
;; the names in the topojson file. I might have missed some!

;; Source for the topojson file on [Github - topojson/worldatlas](https://github.com/topojson/world-atlas)

^{::clerk/visibility {:code :show :result :hide}}
(def DS_country_updated
  (-> DS_country
      (tc/map-columns :country-updated [:Country]
                      (fn [country]
                        (condp = country
                          "United States"                      "United States of America"
                          "Czech Republic"                     "Czechia"
                          "Congo (Democratic Republic Of The)" "Dem. Rep. Congo"
                          "Central African Republic"           "Central African Rep."
                          "Bosnia and Herzegovina"             "Bosnia and Herz."
                          country)))))

;; I couldn't find Eswatini or South Sudan in the dataset.


{::clerk/visibility {:result :hide}}
(def ds_countries_2012_Dec
  (-> DS_country_updated
      (tc/select-rows (comp #(= "2012-12-01" (str %)) :dt))
      (tc/rows :as-maps)))


(def ds_countries_2012_Jul
  (-> DS_country_updated
      (tc/select-rows (comp #(= "2012-07-01" (str %)) :dt))
      (tc/rows :as-maps)))


(def topo-json (slurp "resources/data/topo/countries-110m.json"))


;; Global Picture of Winter and Summer in 2012.
;;


{::clerk/visibility {:result :show}}
(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :data {:format {:feature "countries" :type "topojson"}
         :values topo-json}
  :height 450
  :width 700
  :title "Global Temperatures December 1st 2012"
  :transform [{:lookup "properties.name"
               :from {:data {:values ds_countries_2012_Dec}
                      :fields ["AverageTemperature"]
                      :key "country-updated"}}]
               
  :mark "geoshape"
  :encoding {:color {:field "AverageTemperature" :type "quantitative"}}
  :projection {:type "mercator"}})


(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :data {:format {:feature "countries" :type "topojson"}
         :values topo-json}
  :height 450
  :width 700
  :title "Global Temperatures July 1st 2012"
  :transform [{:lookup "properties.name"
               :from {:data {:values ds_countries_2012_Jul}
                      :fields ["AverageTemperature"]
                      :key "country-updated"}}]

  :mark "geoshape"
  :encoding {:color {:field "AverageTemperature" :type "quantitative"}}
  :projection {:type "mercator"}})

;; ### Hottest and Coldest Countries by Year (Highest/Lowest monthly average recorded)
;;
;; There is a fairly noticable trend of the coldest country (Greenland) warming at
;; a quicker rate since around 1995.

{::clerk/visibility {:result :hide}}
(def hottest-countries-year
  (-> DS_country
      (tc/map-columns :year [:dt] #(str (jt/year %)))
      (tc/group-by :year)
      (tc/order-by [:AverageTemperature] [:desc])
      (tc/aggregate {:highest-avg-temp #(first (% :AverageTemperature))
                     :country #(first (% :Country))})
      (tc/rename-columns {:$group-name :year})
      (tc/drop-missing)))

(def coldest-countries-year
  (-> DS_country
      (tc/drop-missing)
      (tc/map-columns :year [:dt] #(str (jt/year %)))
      (tc/group-by :year)
      (tc/order-by [:AverageTemperature])
      (tc/aggregate {:lowest-avg-temp #(first (% :AverageTemperature))
                     :country #(first (% :Country))})
      (tc/rename-columns {:$group-name :year})))

{::clerk/visibility {:result :show}}
(clerk/vl
 (hc/xform
  ht/hconcat-chart
  :HCONCAT
  [(hc/xform
    ht/bar-chart
    :DATA (-> coldest-countries-year (tc/rows :as-maps))
    :TITLE "<- Lowest Avg. Temperature"
    :X "lowest-avg-temp" :XTYPE "quantitative" :XTITLE nil
    :Y "year" :YTYPE "temporal" :YAXIS nil
    :COLOR "country"
    :HEIGHT 1500
    :WIDTH 250)
   {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
    :data {:values (-> hottest-countries-year (tc/rows :as-maps))}
    :width 10
    :height 1500
    :view {:stroke nil}
    :mark {:type "text"
           :align "center"}
    :encoding {:y {:field "year"
                   :type "temporal"
                   :title ""}}}
   (hc/xform
    ht/bar-chart
    :DATA (-> hottest-countries-year (tc/rows :as-maps))
    :TITLE "Highest Avg. Temperature ->"
    :X "highest-avg-temp" :XTYPE "quantitative" :XTITLE nil
    :Y "year" :YTYPE "temporal" :YAXIS nil
    :COLOR "country"
    :WIDTH 250
    :HEIGHT 1500)]))
;; ### Gap between highest and lowest July temp since 1850, by country
{::clerk/visibility {:result :hide}}
(defn changes-gap [month]
  (-> DS_country_updated
      (tc/drop-rows #(> 1850 (parse-long (str (jt/year (% :dt))))))
      (tc/select-rows #(= month (str (jt/month (% :dt)))))
      (tc/drop-missing :AverageTemperature)
      (tc/group-by :country-updated)
      (tc/order-by [:AverageTemperature])
      (tc/aggregate {:max-val #(last (% :AverageTemperature))
                     :max-dt #(last (% :dt))
                     :min-val #(first (% :AverageTemperature))
                     :min-dt #(first (% :dt))})
      (tc/rename-columns {:$group-name :Country})
      (tc/map-columns :difference [:max-val :min-val] #(- %1 %2))
      (tc/order-by [:difference] [:desc])))

(defn changes-tbl-fmt [ds]
  (-> ds
     (tc/map-columns :max-dt [:max-dt] #(str %))
     (tc/map-columns :min-dt [:min-dt] #(str %))
     (tc/map-columns :max-val [:max-val] #(format "%.2f" %))
     (tc/map-columns :min-val [:min-val] #(format "%.2f" %))
     (tc/map-columns :difference [:difference] #(format "%.2f" %))))

(def july-changes (changes-gap "JULY"))
(def july-changes-tbl-fmt (changes-tbl-fmt july-changes))
(def january-changes (changes-gap "JANUARY"))
(def january-changes-tbl-fmt (changes-tbl-fmt january-changes))


{::clerk/visibility {:result :show}}
;; #### Top 10 changes July
(clerk/table
 (-> july-changes-tbl-fmt (tc/select-rows (range 11))))

;; #### Botton 10 changes July

(clerk/table
 (-> july-changes-tbl-fmt (tc/order-by [:difference]) (tc/select-rows (range 11))))

;; ### Gap between highest and lowest January temp since 1850, by country

;; #### Top 10 changes January
(clerk/table
 (-> january-changes-tbl-fmt (tc/select-rows (range 11))))


;; #### Bottom 10 changes January
(clerk/table
 (-> january-changes-tbl-fmt
     (tc/order-by :difference)
     (tc/select-rows (range 11))))

;; ### January and July changes mapped

;; Maps show the intensity of change from lowest to highest. Darker areas experienced greatest rise in
;; temperature compared with thier lowest since 1850. Europe seems to have experienced the greatest differentials.


{::clerk/visibility {:result :show}}
(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :data {:format {:feature "countries" :type "topojson"}
         :values topo-json}
  :height 450
  :width 700
  :title "High-low difference July"
  :transform [{:lookup "properties.name"
               :from {:data {:values (-> july-changes (tc/rows :as-maps))}
                      :fields ["difference"]
                      :key "Country"}}]
               
  :mark "geoshape"
  :encoding {:color {:field "difference" :type "quantitative"}}
  :projection {:type "mercator"}})

(clerk/vl
 {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
  :data {:format {:feature "countries" :type "topojson"}
         :values topo-json}
  :height 450
  :width 700
  :title "High-low difference January"
  :transform [{:lookup "properties.name"
               :from {:data {:values (-> january-changes (tc/rows :as-maps))}
                      :fields ["difference"]
                      :key "Country"}}]

  :mark "geoshape"
  :encoding {:color {:field "difference" :type "quantitative"}}
  :projection {:type "mercator"}})

;; ### Ireland
;; Just including this here because it is where I live!
;;
;; General trend shows an increase from around a 9 degree average temperature to a 10.3 degree one.
;; Interestingly, the trend looks fairly 'even' until 1900 when it begins to creep up.

{::clerk/visibility {:result :hide}}
(def trend-layer
  (assoc ht/point-chart
         :aerial.hanami.templates/defaults
         {:X :data/x> :XTYPE :xtype>
          :Y :data/y> :YTYPE :ytype>
          :YSCALE {:zero false}
          :DATA hc/RMV
          :WIDTH :width> :HEIGHT :height>
          :USERDATA hc/RMV}))

(def trend-layer-line
  (assoc ht/line-chart
         :aerial.hanami.templates/defaults
         {:X :data/x> :XTYPE :xtype>
          :Y :data/y> :YTYPE :ytype>
          :YSCALE {:zero false}
          :DATA hc/RMV
          :MSIZE 3
          :WIDTH :width> :HEIGHT :height>
          :USERDATA hc/RMV}))

(def trend-chart
  (assoc ht/layer-chart
         :description "A two layer plot of base data and its smoothed trend line given by loess transform"
         :aerial.hanami.templates/defaults
         {:LAYER [(hc/xform trend-layer)
                  (hc/xform
                   trend-layer-line
                   :TRANSFORM [{:loess :data/y> :on :data/x>}]
                   :MCOLOR :trend-color>)]
          :trend-color> "firebrick"
          :xtype> :XTYPE :ytype> :YTYPE
          :width> 700
          :height> (hc/get-defaults :HEIGHT)}))

{::clerk/visibility {:result :show}}
(clerk/vl
 (hc/xform
  trend-chart
  :DATA (-> DS_country
            (tc/drop-missing)
            (tc/select-rows (comp #(= "Ireland" %) :Country))
            (tc/map-columns :season [:dt] #(assign-season %))
            (tc/map-columns :date [:dt] #(str %))
            (tc/rows :as-maps))
  :data/x> :date :xtype> :temporal
  :data/y> :AverageTemperature :ytype> :quantitative
  :COLOR "season"))

;; Apparently ireland _does_ have seasons after all! Although, when you compare with the first
;; chart on seasons, Ireland's seasons are much less clearly delineated. We seem to generally
;; oscillate between 6 and 14 degrees, which seems about right.
;;
(clerk/vl
 (hc/xform
  ht/point-chart
  :DATA
  (-> DS_country
      (tc/drop-missing)
      (tc/select-rows (comp #(= "Ireland" %) :Country))
      (tc/map-columns :season [:dt] #(assign-season %))
      (tc/map-columns :date [:dt] #(str %))
      (tc/rows :as-maps))
  :X "date" :XTYPE "temporal"
  :Y "AverageTemperature" :YTYPE "quantitative"
  :COLOR "season"
  :WIDTH 600))
