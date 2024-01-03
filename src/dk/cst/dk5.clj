(ns dk.cst.dk5
  "Fetch the DK5 dataset and store it as EDN."
  (:require [clojure.walk :as walk]
            [clojure.pprint :as pprint]
            [clojure.data.json :as json]))

(defn fetch-json [url]
  (-> (slurp url)
      (json/read-str)
      (get "result")))

(defn fetch-tree [q]
  (fetch-json (str "https://dk5.dk/api/hierarchy?q=" q)))

(defn fetch-list [q]
  (fetch-json (str "https://dk5.dk/api/list?q=" q)))

(defn fetch-search [q]
  (fetch-json (str "https://dk5.dk/api/search?q=" q
                   "&limit=100&offset=0&sort=relevance&spell=dictionary")))

(defn crawl
  "Crawl the dataset of dk5.dk from any entry point index `q`."
  [q & [existing-structure]]
  (let [structure (atom (or existing-structure {}))
        branches  (atom [])]
    (println "crawling" q)

    ;; Saving all new indices found in a single fetch request.
    ;; This relies on preorder traversal since the JSON data will often contain
    ;; the same index multiple times. The top/first occurrence contains the
    ;; *real* label for the index; subsequent occurrences are subcategories.
    (walk/prewalk
      (fn [x]
        (when (map? x)
          (let [{:strs [index title hasChildren]} x]
            (when (and index title (not (get x "decommissioned")))
              (if (get @structure index)
                (when-not (get @structure title)
                  (swap! structure assoc title {:title title
                                                :index index}))
                (do
                  (swap! structure assoc index {:title title})
                  (when (and (not= index q) hasChildren)
                    (swap! branches conj index)))))))

        x)
      (fetch-tree q))

    ;; New branches are explored sequentially (assuming no network failures).
    ;; Once every branch has been explored, the final structure is returned.
    (let [new-structure @structure
          new-branches  @branches]
      (println "\t..." (- (count new-structure) (count existing-structure))
               "new indices added")
      (println "\t... found" (count new-branches) "new indices to crawl")
      (reduce (fn [m index]
                (merge m (crawl index m)))
              new-structure
              (seq new-branches)))))

(defn proper-indices
  "Separate the proper indices in a `dk5` structure from subcategories."
  [dk5]
  (filter #(re-matches #"[-0-9.]+" %) (keys dk5)))

(defn write-file [f m]
  (spit f (with-out-str (pprint/write m))))

(def dk5
  (delay (crawl "00-07")))

(def dk5-proper
  (delay (select-keys @dk5 (proper-indices @dk5))))

(def dk5-subcategories
  (delay (apply dissoc @dk5 (proper-indices @dk5))))

(comment
  ;; 11327 indices (counting subcategories too) as of 2024-01-03.
  (count indices)

  ;; Write everything to disk.
  (write-file "dk5.edn" @dk5)
  (write-file "dk5-proper.edn" @dk5-proper)
  (write-file "dk5-subcategories.edn" @dk5-subcategories)
  #_.)
