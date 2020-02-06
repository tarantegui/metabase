(ns metabase.query-processor.experimental
  (:require [clojure.core.async :as a]
            [metabase.driver :as driver]))

(defn driver-execute-query [driver query respond context]
  (if (:throw? test-config)
    (throw (ex-info "Oops!" {}))
    (respond {:cols [{:name "col 1"}
                     {:name "col 2"}]}
             [[1 2]
              [3 4]])))

(defn default-finshf [metadata reduced-rows]
  (-> (assoc metadata :data {:rows reduced-rows
                             :cols (:cols metadata)})
      (dissoc :cols)))

(def default-xformf (constantly identity))

(defn default-rff [metadata]
  (let [row-count (volatile! 0)]
    (fn
      ([]
       [metadata []])

      ([[mta rows]]
       {:pre [(map? mta)]}
       [(assoc mta :row_count @row-count)
        rows])

      ([[mta rows] row]
       (vswap! row-count inc)
       [mta (conj rows row)]))))

(defn default-reducef [query xformf {:keys [rff metadata finshf raisef executef], :as context}]
  {:pre [(map? query) (fn? xformf) (fn? rff) (fn? metadata) (fn? finshf) (fn? raisef) (fn? executef)]}
  (executef
   (:driver query)
   query
   (fn respond* [mta reducible-rows]
     (let [mta (metadata mta)]
       (try
         (let [xform (xformf mta)
               rf    (rff mta)
               rf    (xform rf)]
           (let [reduced-result     (transduce identity rf (rf) reducible-rows)
                 [mta reduced-rows] reduced-result]
             (finshf mta reduced-rows)))
         (catch Throwable e
           (raisef e)))))
   context))

(defn default-context
  "Default context used to execute a query. You can override these options as needed."
  []
  { ;; function used to execute the query. Has the signature. By default, `driver/execute-reducible-query`.
   ;;
   ;; (executef driver query context respond) -> (respond metadata reducible-rows)
   :_executef driver/execute-reducible-query
   :executef  driver-execute-query

   ;; gets/transforms final native query before handing off to driver for execution.
   ;;
   ;; (nativef native-query) -> native-query
   :nativef identity

   ;; gets/transforms final preprocessed query before converting to native
   ;;
   ;; (preprocessedf preprocessed-query) -> preprocessed-query
   :preprocessedf identity

   ;; gets results metadata upon query execution and transforms as needed. (Before it is passed to `xformf`)
   ;;
   ;; (metadata results-metadata) -> results-metadata
   :metadata identity

   ;; combines the results metadata and reduced rows in to the final result returned by the QP.
   ;;
   ;; (finshf metadata reduced-rows)
   :finshf default-finshf

   ;; self-explanatory.
   ;;
   ;; (raisef exception)
   :raisef identity

   ;; initial value of `xformf` passed to the first middleware.
   ;;
   ;; (xformf metadata)
   :xformf (constantly identity)

   ;; Function used to reduce the reducible rows in query results.
   ;;
   ;; (rff metadata) -> rf
   :rff default-rff

   ;; Function used to perform reduction of results. Called by the last middleware fn. Equivalent to async ring
   ;; `respond`.
   ;;
   ;; (reducef query xformf context) -> (executef driver query respond context)
   :reducef default-reducef

   ;; sent a message if the query is canceled before finished, e.g. if the connection awaiting results is closed.
   :canceled-chan (a/promise-chan)})

(defn pivot [query xformf {reducef :reducef, :as context}]
  {:pre [(fn? reducef)]}
  (reducef query xformf context))

(defn mbql->native [qp]
  (fn [query xformf {:keys [preprocessedf nativef], :as context}]
    {:pre [(fn? preprocessedf) (fn? nativef)]}
    (let [query        (preprocessedf query)
          native-query (nativef (assoc query :native? true))]
      (qp native-query xformf context))))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                     EXAMPL                                                     |
;;; +----------------------------------------------------------------------------------------------------------------+

(def test-config
  {:throw?  false
   :cancel? false})

(defn example-async-middleware [qp]
  (fn [query xformf {:keys [canceled-chan raisef], :as context}]
    (let [futur (future
                  (try
                    (Thread/sleep 50)
                    (qp query xformf context)
                    (catch Throwable e
                      (println "e:" e)
                      (raisef e)
                      )))]
      (a/go
        (when (a/<! canceled-chan)
          (println "<FUTURE CANCEL>")
          (future-cancel futur))))
    nil))

(defn example-context-xform-middleware [qp]
  (fn [query xformf context]
    (qp query xformf (-> (assoc context :extra-key? true)
                         (update :preprocessedf (fn [preprocessedf]
                                                  (fn [query]
                                                    (println "GOT PREPROCESSED QUERY!" query)
                                                    (preprocessedf query))))))))

(defn example-metadata-xform-middleware [qp]
  (fn [query xformf context]
    (qp query xformf (update context :metadata (fn [metadata]
                                                 (fn [mta]
                                                   (let [mta (metadata mta)]
                                                     (update mta :cols #(for [col %]
                                                                          (assoc col :fancy? true))))))))))

(defn example-query-xform-middleware [qp]
  (fn [query xformf context]
    (qp (assoc query :example-query-xform-middleware? true) xformf context)))

(defn- example-rows-xform-middleware [qp]
  (fn [query xformf context]
    (qp
     query
     (fn example-rows-xform-middleware-xformf [mta]
       {:pre [(map? mta)]}
       (comp
        (fn example-rows-xform-middleware-xform [rf]
          (let [another-row-count (volatile! 0)]
            (fn example-rows-xform-middleware-rf
              ([] (rf))
              ([[mta rows]]
               (rf [(assoc mta :row_count_2 @another-row-count)
                    rows]))

              ([acc row]
               (vswap! another-row-count inc)
               (rf acc (conj row :cool))))))
        (xformf mta)))
     (update context :metadata #(comp % (fn [mta] (update mta :cols conj {:name "Cool col"})))))))

(def pipeline
  (-> pivot
      mbql->native
      example-async-middleware
      example-context-xform-middleware
      example-metadata-xform-middleware
      example-query-xform-middleware
      example-rows-xform-middleware))

(defn build-qp* [qp]
  (fn qp*
    ([query]
     (qp* query nil))

    ([query context]
     (let [{:keys [xformf finshf raisef canceled-chan], :as context} (merge (default-context) context)]
       (assert (fn? xformf))
       (assert (fn? finshf))
       (assert (fn? raisef))
       (assert (some? canceled-chan))
       (let [result-chan (a/promise-chan)
             finshf'     (fn [metadata reduced-rows]
                           (a/>!! result-chan (finshf metadata reduced-rows)))
             raisef'      (fn [e]
                           (a/>!! result-chan (raisef e)))]
         ;; if `result-chan` gets closed before it gets a result, send a message to `canceled-chan`
         (a/go
           (let [[val port] (a/alts! [result-chan canceled-chan] :priority true)]
             (when (and (= val nil)
                        (= port result-chan))
               (a/>! canceled-chan :cancel))
             (a/close! result-chan)
             (a/close! canceled-chan)))
         (qp query xformf (merge context
                                 {:finshf finshf'
                                  :raisef  raisef'}))
         result-chan)))))

(def qp (build-qp* pipeline))

(defn- x []
  (let [result-chan (qp {:my-query? true})]
    (when (:cancel? test-config)
      (a/close! result-chan))
    (let [[val] (a/alts!! [result-chan (a/timeout 1000)])]
      (when (instance? Throwable val)
        (throw val))
      val)))
