;; # Exploring 'Most Streamed Spotify Songs 2023'
;; - Dataset Source: [Kaggle](https://www.kaggle.com/datasets/rajatsurana979/most-streamed-spotify-songs-2023)
;; - Last updated: <2023-10-15 Sun>
;; TODO Clean up and finalize
(ns notebooks.spotify
  {:nextjournal.clerk/visibility {:code :fold}
   :nextjournal.clerk/toc true}
  (:require
   [aerial.hanami.common :as hc]
   [aerial.hanami.templates :as ht]
   [clojure.string :as str]
   [nextjournal.clerk :as clerk]
   [tablecloth.api :as tc]))


^{::clerk/visibility {:code :hide :result :hide}}
(swap! hc/_defaults assoc :BACKGROUND "white")
;; ^{::clerk/visibility {:code :hide :result :hide}}
;; (swap! hc/_defaults assoc :TOOLTIP
;;        [{:field :X, :type :XTYPE, :title :XTTITLE, :format :XTFMT}
;;         {:field :Y, :type :YTYPE, :title :YTTITLE, :format :YTFMT}
;;         {:field "track_name" :type "nominal" :title "Track Name"}
;;         {:field "artists_name" :type "nominal" :title "Artist Name"}
;;         {:field "released_year" :type "nominal" :title "Year Released"}])

^{::clerk/visibility {:code :hide :result :hide}}
(def input-file "resources/data/spotify_2023/spotify-2023.csv")


^{::clerk/visibility {:result :hide}}
(defn remove-brackets-name
  "Helper function for cleaning the column named 'artist(s)_name'"
  [name]
  (str/replace name #"([()])" ""))


^{::clerk/visibility {:result :hide}}
(def ds (-> (tc/dataset input-file {:key-fn (comp keyword remove-brackets-name)})
            (tc/order-by :streams [:desc]) ;; this and below to remove the missing/error 'stream' value
            (tc/drop-rows 0)
            (tc/map-columns :streams [:streams] parse-long)))

;; ## First Look
;; ### Dataset Info

(clerk/table
 (-> ds
     (tc/info :basic)
     (tc/rows :as-maps)))

(clerk/table
 (-> ds tc/info))


;; From the Kaggle page, the meanings of the columns are as follows:
;; - track_name: Name of the song
;; - artist(s)_name: Name of the artist(s) of the song
;; - artist_count: Number of artists contributing to the song
;; - released_year: Year when the song was released
;; - released_month: Month when the song was released
;; - released_day: Day of the month when the song was released
;; - in_spotify_playlists: Number of Spotify playlists the song is included in
;; - in_spotify_charts: Presence and rank of the song on Spotify charts
;; - streams: Total number of streams on Spotify
;; - in_apple_playlists: Number of Apple Music playlists the song is included in
;; - in_apple_charts: Presence and rank of the song on Apple Music charts
;; - in_deezer_playlists: Number of Deezer playlists the song is included in
;; - in_deezer_charts: Presence and rank of the song on Deezer charts
;; - in_shazam_charts: Presence and rank of the song on Shazam charts
;; - bpm: Beats per minute, a measure of song tempo
;; - key: Key of the song
;; - mode: Mode of the song (major or minor)
;; - danceability_%: Percentage indicating how suitable the song is for dancing
;; - valence_%: Positivity of the song's musical content
;; - energy_%: Perceived energy level of the song
;; - acousticness_%: Amount of acoustic sound in the song
;; - instrumentalness_%: Amount of instrumental content in the song
;; - liveness_%: Presence of live performance elements
;; - speechiness_%: Amount of spoken words in the song

;; ### First 10 rows
(clerk/table
 (-> ds
     (tc/select-rows (range 11))))


;; Initial data to look for, based on the info above:
;; - Top ten tracks streamed
;; - Top ten artists streamed
;; - Top ten tracks/artists with releases in 2023
;; - Streams by release year, month, date
;; - Average Streams by Artist Count
;; - Streams by BPM
;; - Streams by dancibility, valence, energy, acousticness, instrumentalness,liveness,speechiness
;;

;; ## Top 10s
;; ### Top 10 Most Streamed Tracks

^{::clerk/visibility {:result :hide}}
(defn format-num [num-str]
  (if (double? num-str)
    (format "%,.0f" num-str)
    (format "%,d" num-str)))

(clerk/table
 (-> ds
     (tc/select-columns [:streams :track_name :artists_name  :released_year])
     (tc/order-by [:streams] [:desc])
     (tc/select-rows (range 11))
     (tc/map-columns :streams [:streams] format-num)))


;; ### Top 10 Most Streamed Artists

^{::clerk/visibility {:result :hide}}
(defn grouped-streams-top-10 [data category]
  (-> data
      (tc/group-by category)
      (tc/aggregate-columns [:streams] #(reduce + %))
      (tc/order-by [:streams] [:desc])
      (tc/rename-columns {:$group-name category})))

^{::clerk/visibility {:result :hide}}
(def top-10-artists-by-streams
  (-> ds
      (grouped-streams-top-10 :artists_name)
      (tc/select-rows (range 11))))

(clerk/table
 (-> top-10-artists-by-streams
     (tc/map-columns :streams [:streams] format-num)))
 
(clerk/vl
 (hc/xform
  ht/bar-chart
  :TITLE "Top 10 Streaming Arists"
  :DATA (-> top-10-artists-by-streams (tc/rows :as-maps))
  :X "artists_name" :XTYPE "nominal" :XSORT "streams"
  :Y "streams"
  :WIDTH 550))

(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA (-> ds
            (tc/group-by :artists_name)
            (tc/aggregate-columns [:track_name] #(count %))
            (tc/order-by [:track_name] [:desc])
            (tc/rename-columns {:track_name :number_of_tracks
                                :$group-name :artist})
            (tc/select-rows (range 26))
            (tc/rows :as-maps))
  :X "artist" :XTYPE "nominal" :XSORT :number_of_tracks
  :Y "number_of_tracks"
  :WIDTH "600"))

(clerk/table
 (-> ds
     (tc/group-by :artists_name)
     (tc/aggregate {:average_streams_per_track #(float (/ (reduce + (% :streams)) (count (% :track_name))))})
     (tc/order-by [:average_streams_per_track] [:desc])
     (tc/select-rows (range 11))))

;; Let's look at the distribution of streaming counts. How much of the streaming time do the top tracks eat up?

(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA (-> ds
            (tc/order-by [:streams] [:desc])
            (tc/group-by {"A Top 150" (range 151)
                          "B 151-951" (range 151 952)})
            (tc/aggregate-columns [:streams] #(reduce + %))
            (tc/rename-columns {:$group-name :group})
            (tc/rows :as-maps))
  :X "group"  :XTYPE "nominal"
  :Y "streams"
  :WIDTH "600"))

;; The top 150 Tracks have roughly the same amount of streams as the remaining 800.

;; All Tracks Distribution
(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA (-> ds
            (tc/order-by [:streams] [:desc])
            (tc/add-column :ranking (range))
            (tc/rows :as-maps))
  :X "ranking" :XTYPE "ordinal" :XAXIS {:labels false}
  :Y "streams"
  :WIDTH "600"))

;; Top 20 Distribution
;; 

(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA (take 20 (-> ds
                     (tc/order-by [:streams] [:desc])
                     (tc/add-column :ranking (range))
                     (tc/rows :as-maps)))
  :X "ranking" :XTYPE "ordinal" :XAXIS {:labels false}
  :Y "streams"
  :WIDTH 500
  :COLOR "artists_name"))
;; ### Top 10 Tracks Released in 2023


(clerk/table
 (-> ds
     (tc/select-columns [:streams :track_name :artists_name  :released_year :released_month])
     (tc/select-rows (comp #(= 2023 %) :released_year))
     (tc/drop-columns [:released_year])
     (tc/order-by [:streams] [:desc])
     (tc/select-rows (range 11))
     (tc/map-columns :streams [:streams] format-num)))

;; ### Top 10 Artists with Releases in 2023

(clerk/table
 (-> ds
     (tc/select-columns [:streams :artists_name :released_year])
     (tc/select-rows (comp #(= 2023 %) :released_year))
     (tc/drop-columns [:released_year])
     (tc/group-by :artists_name)
     (tc/aggregate-columns [:streams] #(reduce + %))
     (tc/order-by [:streams] [:desc])
     (tc/rename-columns {:$group-name :artist})
     (tc/select-rows (range 11))
     (tc/map-columns :streams [:streams] format-num)))

;; ## Release Dates

^{::clerk/visibility {:result :hide}}
(def top-10-by-year-released (-> ds (grouped-streams-top-10 :released_year)))
^{::clerk/visibility {:result :hide}}
(def top-10-by-month-released (-> ds (grouped-streams-top-10 :released_month)))
^{::clerk/visibility {:result :hide}}
(def top-10-by-day-released (-> ds (grouped-streams-top-10 :released_day)))

;; It seems like 2022 was a good year for music...

(clerk/vl
 (hc/xform
  ht/bar-chart
  :TITLE "Top Streaming by Year Released"
  :DATA (->  top-10-by-year-released (tc/rows :as-maps))
  :X "released_year" :XTYPE "ordinal"
  :Y "streams"
  :WIDTH 600))

(clerk/vl
 (hc/xform
  ht/bar-chart
  :TITLE "Top Streaming by Year Released (Past 20 years)"
  :DATA (->  top-10-by-year-released
             (tc/select-rows (comp #(some #{%} (range 2003 2024)) :released_year))
             (tc/rows :as-maps))
  :X "released_year" :XTYPE "ordinal"
  :Y "streams"
  :WIDTH 600))

(clerk/vl
 (hc/xform
  ht/bar-chart
  :TITLE "Top Streaming by Year Released (More than 20 Years old)"
  :DATA (->  top-10-by-year-released
             (tc/drop-rows (comp #(some #{%} (range 2003 2024)) :released_year))
             (tc/rows :as-maps))
  :X "released_year" :XTYPE "ordinal"
  :Y "streams"
  :WIDTH 600))
;; Let's look at some of those top tracks from the most popular older years (2002, 1999, 1984, 1975)

(clerk/table
 (-> ds
     (tc/select-rows (comp #(some #{%} [2002 1999 1984 1975]) :released_year))
     (tc/select-columns [:streams :track_name :artists_name :released_year])
     (tc/order-by [:streams] [:desc])
     (tc/select-rows (range 11))
     (tc/map-columns :streams [:streams] format-num)))

;; I'm not sure how Vance Joy managed to release that song 12 years before he was born! :/ Release date should be 2013.
;;

(clerk/vl
 (hc/xform
  ht/bar-chart
  :TITLE "Top Streaming by Month of Year Released"
  :DATA (->  top-10-by-month-released (tc/rows :as-maps))
  :X "released_month" :XTYPE "ordinal"
  :Y "streams"
  :WIDTH 600))

(clerk/vl
 (hc/xform
  ht/bar-chart
  :TITLE "Top Streaming by Day of Month Released"
  :DATA (->  top-10-by-day-released (tc/rows :as-maps))
  :X "released_day" :XTYPE "ordinal"
  :Y "streams"
  :WIDTH 600))

;; Is the best time to release a song the 1st of January??
;; These results seem too skewed toward January and especially the first of the month.
;; It is more likely these values were provided automatically for missing values.
;; Let's look at a few of these.

(clerk/table
 (-> ds
     (tc/select-rows (comp #(= 1 %) :released_month))
     (tc/select-rows (comp #(= 1 %) :released_day))
     (tc/select-columns [:streams :track_name :artists_name :released_year :released_month :released_day])
     (tc/order-by [:streams] [:desc])
     (tc/select-rows (range 11))))


;; TODO Finish adding the correct info for tracks in here, to proove the source data is incorrect.


;; ## Collaborations
;; Looking at how artists collabs impact streams


(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA (-> ds
            (tc/group-by :artist_count)
            (tc/aggregate-columns [:streams] #(float (/ (reduce + %) (count %))))
            (tc/rename-columns {:streams "Average Streams"
                                :$group-name "Number of Artists"})
            (tc/rows :as-maps))
  :TITLE "Average Streams by Artist Count on Track"
  :X "Number of Artists" :XTYPE "nominal"
  :Y "Average Streams"
  :WIDTH 600))

;; Single arists perform best on average, and if you are going for large collabs, better to go with 7 artists than 5, 6, or 8!

;; ## Beats per Minute

(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA (-> ds
            (tc/group-by :bpm)
            (tc/aggregate-columns [:streams] #(float (/ (reduce + %) (count %))))
            (tc/rename-columns {:$group-name "BPM"
                                :streams "Average Streams"})
            (tc/rows :as-maps))
  :TITLE "Average Streams by BPM"
  :X "BPM"
  :Y "Average Streams"
  :WIDTH 600))

(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA (-> ds
            (tc/group-by :bpm)
            (tc/aggregate-columns [:streams] #(reduce + %))
            (tc/rename-columns {:$group-name "BPM"
                                :streams "Total Streams"})
            (tc/rows :as-maps))
  :TITLE "Total Streams by BPM"
  :X "BPM"
  :Y "Total Streams"
  :WIDTH 600))

;; Unsurprisingly, 120 BPM has the highest streaming rate.
;; More surpisingly is that tracks at 170-171 BPM seem to do well also.
;;
;; Let's look at the top tracks at those rates.

;; ### Tracks at 170/171 BPM

(clerk/table
 (-> ds
     (tc/select-rows (comp #(or (= 170 %) (= 171 %)) :bpm))
     (tc/order-by [:streams] [:desc])
     (tc/select-columns[:streams :track_name :artists_name :bpm  :released_year :dancibility_%])
     (tc/map-columns :streams [:streams] format-num)))

;; A low number of tracks at this rate, but average skewed some high-performing artists/tracks in this category. We also see a double-entry here for 'Rosa Linn'...
;;
;; These songs are also relatively recent releases (since 2019)

;; ### BPM and song qualities

^{::clerk/visibility {:result :hide}}
(def qualities [:danceability_%
                :valence_%
                :energy_%
                :acousticness_%
                :instrumentalness_%
                :liveness_%
                :speechiness_%])

^{::clerk/visibility {:result :hide}}
(defn average-qualities [ds group]
  (-> ds
      (tc/group-by group)
      (tc/aggregate-columns qualities #(float (/ (reduce + %) (count %))))
      (tc/rename-columns {:$group-name group})))


^{::clerk/visibility {:result :hide}}
(defn split-avg-qual-map [entries]
  (let [create-sub-map-quality (fn [quality entry]
                                 (-> {}
                                     (assoc :bpm (:bpm entry))
                                     (assoc :bin (:bin-bpm entry))
                                     (assoc :value (quality entry))
                                     (assoc :type (name quality))))]
    (reduce (fn [result entry]
              (into result
                    (map #(create-sub-map-quality % entry) qualities)))
            []
            entries)))

^{::clerk/visibility {:result :hide}}
(def split-qualities-bpm (split-avg-qual-map
                          (-> (average-qualities ds :bpm) (tc/rows :as-maps))))
;; All qualities by BPM:

(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA split-qualities-bpm
  :X "bpm"
  :Y "value" :YTITLE "Average %"
  :COLOR "type"
  :WIDTH "600"))

;; Filtering for the qualities with the highest % (Danceability, Valence, Energy)

(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA (filter #(some #{(:type %)} ["danceability_%"
                                     "valence_%"
                                     "energy_%"]) split-qualities-bpm)
  :X "bpm"
  :Y "value" :YTITLE "Average %"
  :COLOR "type"
  :WIDTH "600"))

;; Danceability only:

(clerk/vl
 (hc/xform
  ht/line-chart
  :DATA (filter #(some #{(:type %)} ["danceability_%"]) split-qualities-bpm)
  :X "bpm"
  :Y "value" :YTITLE "Average %"
  :COLOR "type"
  :WIDTH "600"
  :POINT true))


;; There is a strange spike in Danceability at 73 BPM. Lets find those tracks:

(clerk/table
 (-> ds
     (tc/select-rows (comp #(= 73 %) :bpm))))

;; Just one track! No wonder it skewed the average.
;;
;; Instead, lets try grouping the BPM into buckets of 10

^{::clerk/visibility {:result :hide}}
(defn assign-bpm-group [int]
  (let [s (str int)
        firsts (apply str (if (= 3 (count s)) (take 2 s) (into [\0] (take 1 s))))
        upper (inc (parse-long firsts))]
    (str firsts "0-" (str upper "0"))))


^{::clerk/visibility {:result :hide}}
(def split-qualities-bpm-bin (split-avg-qual-map
                              (-> ds
                                  (tc/map-columns :bin-bpm [:bpm] assign-bpm-group)
                                  (average-qualities :bin-bpm)
                                  (tc/rows :as-maps))))

(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA split-qualities-bpm-bin
  :X "bin" :XTYPE "ordinal"
  :Y "value" :YTITLE "Average %"
  :COLOR "type"
  :WIDTH "600"))


(clerk/vl
 (hc/xform
  ht/line-chart
  :DATA (filter #(some #{(:type %)} ["danceability_%"
                                     "energy_%"]) split-qualities-bpm-bin)
  :X "bin" :XTYPE "ordinal"
  :Y "value" :YTITLE "Average %"
  :COLOR "type"
  :WIDTH "600"
  :POINT true))

;; Still a faily even spread in terms of the qualities associated with BPMs. It seems like if you want good danceability and energy, you should stick to songs in the 90-140 BPM range.
;; Nothing too insightful there.
;;
;; It is interesting to see a high percentage of 'acousticness' at the 200+ BPM Range.
;; Let's have a look at some tracks in that range

(clerk/table
 (-> ds
     (tc/select-rows (comp #(> % 200) :bpm))
     (tc/order-by [:acousticness_%] [:desc])
     (tc/select-columns [:track_name :artists_name :bpm :acousticness_% :streams])))

;; Now, I'm not a musicologist, but I find it strange to see 'Taylor Swift - Lover' down as 206 BPM.
;; Maybe this is technically true, or maybe it is a problem when using machines to assign these kind of values.
;; For me, this song has the feeling of being sub-100 BPM. It is similar for the other songs I think.

;; ### Danceability, Energy and BPM
;; As expected, danceability and engergy go together for the most part.

(clerk/vl
 (hc/xform
  ht/point-chart
  :DATA (-> ds
            (tc/map-columns :bin-bpm [:bpm] assign-bpm-group)
            (tc/rows :as-maps))
  :X "danceability_%"
  :Y "energy_%"
  :COLOR "bin-bpm"
  :TOOLTIP [{:field :X, :type :XTYPE, :title :XTTITLE, :format :XTFMT}
            {:field :Y, :type :YTYPE, :title :YTTITLE, :format :YTFMT}
            {:field "track_name" :type "nominal" :title "Track Name"}
            {:field "bpm" :type "nominal" :title "bpm"}
            {:field "artists_name" :type "nominal" :title "Artist Name"}]
  :WIDTH "600"))

;; ## Song Qualities
;;
;; ### Danceability and Acousticness
;;
;; Are songs less danceable when they are more acousitc?

(clerk/vl
 (hc/xform
  ht/point-chart
  :DATA (-> ds (tc/rows :as-maps))
  :X "danceability_%"
  :Y "acousticness_%"
  :TOOLTIP [{:field :X, :type :XTYPE, :title :XTTITLE, :format :XTFMT}
            {:field :Y, :type :YTYPE, :title :YTTITLE, :format :YTFMT}
            {:field "track_name" :type "nominal" :title "Track Name"}
            {:field "released_year" :type "nominal" :title "Year"}
            {:field "artists_name" :type "nominal" :title "Artist Name"}]
  :WIDTH "600"))

;; Seems to be the case, most songs with high danceability have low acousticness.

;; ### Liveness and Energy
;; How about liveness and energy?
;;

(clerk/vl
 (hc/xform
  ht/point-chart
  :DATA (-> ds (tc/rows :as-maps))
  :X "liveness_%"
  :Y "energy_%"
  :TOOLTIP [{:field :X, :type :XTYPE, :title :XTTITLE, :format :XTFMT}
            {:field :Y, :type :YTYPE, :title :YTTITLE, :format :YTFMT}
            {:field "track_name" :type "nominal" :title "Track Name"}
            {:field "released_year" :type "nominal" :title "Year"}
            {:field "artists_name" :type "nominal" :title "Artist Name"}]
  :WIDTH "600"))

;; Seems like most high-energy tracks have low liveness. Are electronic tracks just better at producing energy?
;; No, it's just that most of these songs have low liveness ratings in general.
;; It's more of an indication of how 'liveness' doesn't feature too prominently in the streaming landscape.
;; Let's take a detour and look at 'liveness' by year released.

(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA (-> ds
            (tc/group-by :released_year)
            (tc/aggregate-columns [:liveness_%] #(float (/ (reduce + %) (count %))))
            (tc/rename-columns {:$group-name :year})
            (tc/rows :as-maps))
  :X "year"  :XTYPE "ordinal"
  :Y "liveness_%"
  :WIDTH "600"))

;; Let's have a look at the 1971 songs to see what it takes for a track to have high 'liveness'.

(clerk/table
 (-> ds
     (tc/select-rows (comp #(= 1971 %) :released_year))
     (tc/select-columns [:track_name :artists_name :liveness_% :streams])))

;; Just 1 track! Okay, instead lets just look at all tracks with a liveness score over 60%

(clerk/table
 (-> ds
     (tc/select-rows (comp #(> % 60) :liveness_%))
     (tc/order-by [:liveness_%] [:desc])
     (tc/select-columns [:track_name :artists_name :liveness_% :streams])))

;; Still not sure what constitutes 'liveness'. The top track here seems to be an actual live
;; performance of a song, which makes sense, but then you also have songs like the Black Pink
;; one which has a liveness percentage of 63%, but which doesn't seem particularly 'live'.

(clerk/html
 [:iframe {:width 700
           :height 400
           :src "https://www.youtube.com/embed/UhxW9Njqqu0"
           :title "BLACKPINK - ‘Typa Girl’ (Official Audio)"}])

;; ### Qualities Profiles
;; Let's have a closer look at the qualities, outside of the intersection with BPM.
;;
;; Let's build profiles of the top 10 streaming tracks.

^{::clerk/visibility {:result :hide}}
(def top-10
  (-> ds
      (tc/order-by [:streams] [:desc])
      (tc/select-rows (range 11))))

;; #### Aggregate Profile Top 10 Tracks

(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA
  (for [q qualities
        :let [vals (q top-10)]]
    {:quality q
     :average (float (/ (reduce + vals) (count vals)))})
  :X "quality" :XTYPE "nominal" :XSORT "average"
  :Y "average"
  :WIDTH 600))

;; Unsurprisingly, the top 10 tracks have an average of **68.8%** 'Danceability'

^{::clerk/visibility {:result :hide}}
(defn top-qualities-split [entries]
  (let [submap-fn (fn [entry quality]
                    (-> {}
                        (assoc :track (:track_name entry))
                        (assoc :value (quality entry))
                        (assoc :type quality)))]
    (reduce (fn [result entry]
              (into result
                    (map #(submap-fn entry %) qualities)))
            []
            entries)))
                    

;; #### Individual Track Profiles
(clerk/vl
 (hc/xform
  ht/bar-chart
  :DATA
  (top-qualities-split
   (-> top-10
       (tc/select-columns (into qualities [:track_name :streams]))
       (tc/rows :as-maps)))
  :X "track" :XTYPE "nominal" :XSORT "streams" :XTITLE "Track Title, ordered by streams"
  :Y "value"
  :COLOR "type"
  :WIDTH 500))

;; The top track actually had lower than average danceability (**50%**), but a high level of energy (**80%**)


;;
;; ## Inclusion in Playlists
;; Let's look at the distribution of these songs by inclusion in spotify playlists.


^{::clerk/visibility {:result :hide}}
(def trend-layer-line
  (assoc ht/line-chart
         :aerial.hanami.templates/defaults
         {:X :data/x> :XTYPE :xtype>
          :Y :data/y> :YTYPE :ytype>
          :YSCALE {:zero false}
          :DATA hc/RMV
          :WIDTH :width> :HEIGHT :height>
          :USERDATA hc/RMV}))

^{::clerk/visibility {:result :hide}}
(def trend-layer
  (assoc ht/point-chart
         :aerial.hanami.templates/defaults
         {:X :data/x> :XTYPE :xtype>
          :Y :data/y> :YTYPE :ytype>
          :YSCALE {:zero false}
          :DATA hc/RMV
          :WIDTH :width> :HEIGHT :height>
          :USERDATA hc/RMV}))

^{::clerk/visibility {:result :hide}}
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
          :width> 600
          :height> (hc/get-defaults :HEIGHT)}))

(clerk/vl
 (hc/xform
  trend-chart
  :DATA (-> ds (tc/rows :as-maps))
  :data/x> :in_spotify_playlists
  :data/y> :streams))


;; As expected, being included in spotify playlists increases you streaming count!
;; The effect does taper off however.
;;
;; How about the other playlists?


(clerk/vl
 (hc/xform
  ht/vconcat-chart
  :VCONCAT
  [
   (hc/xform
    trend-chart
    :DATA (-> ds (tc/rows :as-maps))
    :data/x> :in_apple_playlists
    :data/y> :streams)
   (hc/xform
    trend-chart
    :DATA (-> ds (tc/rows :as-maps))
    :data/x> :in_deezer_playlists
    :data/y> :streams)]))

;; Being included in Apple playlists seems more effective than Deezer.

;; ## Conclusion
;;
;; This was just an exploratory look into the dataset. I'm still learning about the
;; various libraries I've used here, and I haven't gone into depth around anything
;; statistical. As you see, at the beginning I was mainly interested in seeing the
;; 'Top 10' of everything!
;;
;; People would probably examine a dataset like this to discover things like 'what makes a track
;; more "streamable"'. If this were the question, there is nothing too surprising or insightful
;; in what I have looked at. You basically should have a song with a high danceability/energy/valence
;; rating and make sure it is included in as many playlists as possible!
;;
;; There were also a few errors in the dataset that cropped up. I still don't know much about
;; techniques around identifying and cleaning these types of errors. Something for next time.
