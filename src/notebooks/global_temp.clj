(ns notebooks.global-temp
  {:nextjournal.clerk/visibility {:code :fold}
   :nextjournal.clerk/toc true}
  (:require
   [aerial.hanami.common :as hc]
   [aerial.hanami.templates :as ht]
   [clojure.string :as str]
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]
   [java-time.api :as jt]
   [cheshire.core :as json]))

;; # Global Temperatures
;; - Source: [Global Temperatures Dataset on Kaggle](https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data)
;;
;; A dataset containing information about global temperatures from 1750 to 2015. It is a bit out of date now,
;; but I'm exploring it as a learning exercise.
;;
;; There are also complementary files containing the same info by Country, City, etc.

^{::clerk/visibility {:result :hide}}
(swap! hc/_defaults assoc :BACKGROUND "white")

^{::clerk/visibility {:result :hide}}
(def global_temperatures_file "resources/data/global_temperatures/GlobalTemperatures.csv")

^{::clerk/visibility {:result :hide}}
(def country_temperatures_file "resources/data/global_temperatures/GlobalLandTemperaturesByCountry.csv")

^{::clerk/visibility {:result :hide}}
(def DS (-> global_temperatures_file
            (tc/dataset {:key-fn keyword})))

^{::clerk/visibility {:result :hide}}
(def DS_country (-> country_temperatures_file
                    (tc/dataset {:key-fn keyword})))


;; ## Dataset Info
;; The main dataset has 3192 rows and 9 columns.

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
;; First, I need to convert the `dt` columns to strings, since I'm not sure how to get hanami
;; work with the java datetime format...

^{::clerk/visibility {:result :hide
                      :code :show}}
(def DS_A (-> DS
              (tc/map-columns :date [:dt] #(str %))))


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


^{::clerk/visibility {:result :hide}}
(def DS_B
  (-> DS_A
      (tc/map-columns :season [:dt] #(assign-season %))))


(clerk/vl
 (hc/xform
  ht/point-chart
  :DATA (-> DS_B (tc/rows :as-maps))
  :X "date" :XTYPE "temporal"
  :Y "LandAverageTemperature"
  :COLOR "season"
  :WIDTH 600))

;; ## Yearly Averages

(def DS_C
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
    :DATA (-> DS_C (tc/rows :as-maps))
    :X "year" :XTYPE "temporal"
    :Y :annual_avg_incl_ocean
    :MSIZE 5
    :MCOLOR "purple"
    :WIDTH 600)
   (hc/xform
    ht/line-chart
    :DATA (-> DS_C (tc/rows :as-maps))
    :X "year" :XTYPE "temporal"
    :Y "annual_avg"
    :MCOLOR "green"
    :WIDTH 600)
   (hc/xform
    ht/line-chart
    :DATA (-> DS_C (tc/rows :as-maps))
    :X "year" :XTYPE "temporal"
    :Y "max_avg_annual"
    :MCOLOR "firebrick")
   (hc/xform
    ht/line-chart
    :DATA (-> DS_C (tc/rows :as-maps))
    :X "year" :XTYPE "temporal"
    :Y "min_avg_annual")]))

;; As we can see, over the period of about 160 years, the average for the min,average, and max annual land temperatures
;; rose by around 2 degrees celsius. The average for the land and ocean temperatures combined (purple line) rose by
;; around 1 degree celsius.

;; ## Highest Max Temperatures
;; Let's look at the highest temperatures on record.

(clerk/table
 (-> DS_B
     (tc/order-by [:LandMaxTemperature] [:desc])
     (tc/select-rows (range 11))
     (tc/select-columns [:date :LandMaxTemperature])))

;; Nine out of ten of the top days were in July and the oldest year was 1998.
;;

;; ### Higest Global Max and Lowest Min Temps for each year

^{::clerk/visibility {:result :hide}}
(def DS_grouped_annual
  (-> DS_B
      (tc/map-columns :year [:dt] #(jt/year %))
      (tc/map-columns :month [:dt] #(jt/month %))
      (tc/drop-missing :LandMaxTemperature)
      (tc/map-columns :year [:year] #(str %))
      (tc/map-columns :month [:month] #(str %))
      (tc/group-by :year)))

^{::clerk/visibility {:result :hide}}
(def yearly-max
  (-> DS_grouped_annual
      (tc/order-by [:LandMaxTemperature] [:desc])
      (tc/aggregate {:yearly-max #(first (% :LandMaxTemperature))
                     :max-month #(first (% :month))})))

^{::clerk/visibility {:result :hide}}
(def yearly-min
  (-> DS_grouped_annual
      (tc/order-by [:LandMinTemperature])
      (tc/aggregate {:yearly-min #(first (% :LandMinTemperature))
                     :min-month #(first (% :month))})))

^{::clerk/visibility {:result :hide}}
(def max-min-join
  (-> (tc/inner-join yearly-max yearly-min :$group-name)
      (tc/rename-columns {:$group-name :year})
      (tc/map-columns :min-max-gap [:yearly-max :yearly-min] #(- %1 %2))
      (tc/rows :as-maps)))

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
  

(clerk/vl
 (hc/xform
  ht/line-chart
  :DATA monthly-DS
  :X "date" :XTYPE "temporal"
  :Y "LandAverageTemperature"
  :COLOR ht/default-color :CFIELD "month" :CTYPE "ordinal" 
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

;; Taking only the years since 1900 (blue line is most recent year, 2015):


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

;;
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


;; ## Increases for different intervals
;;
;; Let's try group the years into 30-year blocks. Then let's look at:
;; - A. The average temperatures for those ranges
;; - B. The increase between the max avearages in the ranges
;;
;; ### A. Average for 30-year ranges

^{::clerk/visibility {:result :hide}}
(def year-ranges {"1986-2015" (range 1986 2016)
                  "1956-1985" (range 1956 1986)
                  "1926-1955" (range 1926 1956)
                  "1896-1925" (range 1896 1926)})

^{::clerk/visibility {:result :hide}}
(defn lookup-year-range [dt ranges]
  (let [year (parse-long (str (jt/year dt)))]
    (key (first (filter (fn [[k v]] (some #{year} v)) ranges)))))

^{::clerk/visibility {:result :hide}}
(def DS_year_ranges
  (-> DS
      (tc/drop-rows (comp #(< (parse-long (str (jt/year %))) 1896) :dt))
      (tc/map-columns :year-range [:dt] #(lookup-year-range % year-ranges))
      (tc/group-by :year-range)))
      

(clerk/vl
 (hc/xform
  ht/hconcat-chart
   :HCONCAT [(hc/xform
              ht/bar-chart
              :DATA
              (-> DS_year_ranges
                  (tc/aggregate {:average_temp
                                 #(float (/
                                          (reduce + (% :LandAverageTemperature))
                                          (count (% :LandAverageTemperature))))})
                  (tc/rename-columns {:$group-name :year-range})
                  (tc/rows :as-maps))
              :X "year-range" :XTYPE "nominal"
              :Y "average_temp"
              :WIDTH 300)
             (hc/xform
              ht/bar-chart
              :DATA
              (-> DS_year_ranges
                  (tc/aggregate {:average_max_temp
                                 #(float (/
                                          (reduce + (% :LandMaxTemperature))
                                          (count (% :LandMaxTemperature))))})
                  (tc/rename-columns {:$group-name :year-range})
                  (tc/rows :as-maps))
              :X "year-range" :XTYPE "nominal"
              :Y "average_max_temp"
              :WIDTH 300)]))


;; We see much sharper increased in the most recent 30 year block than between the previous two years.
;; Let's try 15-year groupings



^{::clerk/visibility {:result :hide}}
(def fifteen-year-ranges
  {"2001-2015" (range 2001 2016)
   "1986-2000" (range 1986 2001)
   "1971-1985" (range 1971 1986)
   "1956-1970" (range 1956 1971)
   "1941-1955" (range 1941 1956)
   "1926-1940" (range 1926 1941)
   "1911-1925" (range 1911 1926)
   "1896-1910" (range 1896 1911)})


^{::clerk/visibility {:result :hide}}
(def DS_fifteen_year_ranges
  (-> DS
      (tc/drop-rows (comp #(< (parse-long (str (jt/year %))) 1896) :dt))
      (tc/map-columns :year-range [:dt] #(lookup-year-range % fifteen-year-ranges))
      (tc/group-by :year-range)))

(clerk/vl
 (hc/xform
  ht/vconcat-chart
  :VCONCAT [(hc/xform
             ht/hconcat-chart
             :HCONCAT [(hc/xform
                        ht/bar-chart
                        :DATA
                        (-> DS_fifteen_year_ranges
                            (tc/aggregate {:average_temp
                                           #(float (/
                                                    (reduce + (% :LandAverageTemperature))
                                                    (count (% :LandAverageTemperature))))})
                            (tc/rename-columns {:$group-name :year-range})
                            (tc/rows :as-maps))
                        :X "year-range" :XTYPE "nominal"
                        :Y "average_temp"
                        :YSCALE {:domain [0, 16]}
                        :WIDTH 300)
                       (hc/xform
                        ht/bar-chart
                        :DATA
                        (-> DS_fifteen_year_ranges
                            (tc/aggregate {:average_max_temp
                                           #(float (/
                                                    (reduce + (% :LandMaxTemperature))
                                                    (count (% :LandMaxTemperature))))})
                            (tc/rename-columns {:$group-name :year-range})
                            (tc/rows :as-maps))
                        :X "year-range" :XTYPE "nominal"
                        :Y "average_max_temp"
                        :WIDTH 300)])
            (hc/xform
             ht/hconcat-chart
             :HCONCAT [(hc/xform
                        ht/bar-chart
                        :DATA
                        (-> DS_fifteen_year_ranges
                            (tc/aggregate {:average_temp_land_ocean
                                           #(float (/
                                                    (reduce + (% :LandAndOceanAverageTemperature))
                                                    (count (% :LandAndOceanAverageTemperature))))})
                            (tc/rename-columns {:$group-name :year-range})
                            (tc/rows :as-maps))
                        :X "year-range" :XTYPE "nominal"
                        :Y "average_temp_land_ocean"
                        :WIDTH 300)
                       (hc/xform
                        ht/bar-chart
                        :DATA
                        (-> DS_fifteen_year_ranges
                            (tc/aggregate {:average_min_temp
                                           #(float (/
                                                    (reduce + (% :LandMinTemperature))
                                                    (count (% :LandMinTemperature))))})
                            (tc/rename-columns {:$group-name :year-range})
                            (tc/rows :as-maps))
                        :X "year-range" :XTYPE "nominal"
                        :Y "average_min_temp"
                        :YSCALE {:domain [0, 16]}
                        :WIDTH 300)])]))

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


^{::clerk/visibility {:result :hide}}
(def ds_countries_2012_Dec
  (-> DS_country_updated
      (tc/select-rows (comp #(= "2012-12-01" (str %)) :dt))
      (tc/rows :as-maps)))


^{::clerk/visibility {:result :hide}}
(def ds_countries_2012_Jul
  (-> DS_country_updated
      (tc/select-rows (comp #(= "2012-07-01" (str %)) :dt))
      (tc/rows :as-maps)))


(def topo-json (slurp "resources/data/topo/countries-110m.json"))


;; Global Picture of Winter and Summer in 2012.
;;


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

^{::clerk/visibility {:result :hide}}
(def hottest-countries-year
  (-> DS_country
      (tc/map-columns :year [:dt] #(str (jt/year %)))
      (tc/group-by :year)
      (tc/order-by [:AverageTemperature] [:desc])
      (tc/aggregate {:highest-avg-temp #(first (% :AverageTemperature))
                     :country #(first (% :Country))})
      (tc/rename-columns {:$group-name :year})
      (tc/drop-missing)))



^{::clerk/visibility {:result :hide}}
(def coldest-countries-year
  (-> DS_country
      (tc/drop-missing)
      (tc/map-columns :year [:dt] #(str (jt/year %)))
      (tc/group-by :year)
      (tc/order-by [:AverageTemperature])
      (tc/aggregate {:lowest-avg-temp #(first (% :AverageTemperature))
                     :country #(first (% :Country))})
      (tc/rename-columns {:$group-name :year})))
      


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
    :HEIGHT 2000
    :WIDTH 250)
   {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
    :data {:values (-> hottest-countries-year (tc/rows :as-maps))}
    :width 10
    :height 2000
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
    :HEIGHT 2000)]))

;; ### Ireland

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
  :Y "AverageTemperature" :YTPE "quantitative"
  :COLOR "season"
  :WIDTH 600))
