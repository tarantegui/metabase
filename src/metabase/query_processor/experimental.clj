(ns metabase.query-processor.experimental
  (:refer-clojure :exclude [reduce])
  (:require [clojure.core.async :as a]
            [metabase.driver :as driver]))

(defn driver-execute-query [driver query respond context]
  (if (:throw? test-config)
    (throw (ex-info "Oops!" {}))
    (respond {:cols [{:name "col 1"}
                     {:name "col 2"}]}
             [[1 2]
              [3 4]])))

(defn default-finish [metadata reduced-rows]
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

(defn default-reduce [query xformf {:keys [rff metadata finish raise execute], :as context}]
  {:pre [(map? query) (fn? xformf) (fn? rff) (fn? metadata) (fn? finish) (fn? raise) (fn? execute)]}
  (execute
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
             (finish mta reduced-rows)))
         (catch Throwable e
           (raise e)))))
   context))

(defn default-context
  "Default context used to execute a query. You can override these options as needed."
  []
  { ;; function used to execute the query. Has the signature. By default, `driver/execute-reducible-query`.
   ;;
   ;; (execute-query driver query context respond) -> (respond metadata reducible-rows)
   :_execute driver/execute-reducible-query
   :execute  driver-execute-query

   ;; gets/transforms final native query before handing off to driver for execution.
   ;;
   ;; (native native-query) -> native-query
   :native identity

   ;; gets/transforms final preprocessed query before converting to native
   ;;
   ;; (preprocessed preprocessed-query) -> preprocessed-query
   :preprocessed identity

   ;; gets results metadata upon query execution and transforms as needed. (Before it is passed to `xformf`)
   ;;
   ;; (metadata results-metadata) -> results-metadata
   :metadata identity

   ;; combines the results metadata and reduced rows in to the final result returned by the QP.
   ;;
   ;; (finish metadata reduced-rows)
   :finish default-finish

   ;; self-explanatory.
   ;;
   ;; (raise exception)
   :raise identity

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
   ;; (reducef query xformf context) -> (execute driver query respond context)
   :reduce default-reduce

   ;; sent a message if the query is canceled before finished, e.g. if the connection awaiting results is closed.
   :canceled-chan (a/promise-chan)})

(defn pivot [query xformf {reducef :reduce, :as context}]
  {:pre [(fn? reducef)]}
  (reducef query xformf context))

(defn mbql->native [qp]
  (fn [query xformf {:keys [preprocessed native], :as context}]
    {:pre [(fn? preprocessed) (fn? native)]}
    (let [query        (preprocessed query)
          native-query (native (assoc query :native? true))]
      (qp native-query xformf context))))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                     EXAMPL                                                     |
;;; +----------------------------------------------------------------------------------------------------------------+

(def test-config
  {:throw?  false
   :cancel? false})

(defn example-async-middleware [qp]
  (fn [query xformf {:keys [canceled-chan raise], :as context}]
    (let [futur (future
                  (try
                    (Thread/sleep 50)
                    (qp query xformf context)
                    (catch Throwable e
                      (println "e:" e)
                      (raise e)
                      )))]
      (a/go
        (when (a/<! canceled-chan)
          (println "<FUTURE CANCEL>")
          (future-cancel futur))))
    nil))

(defn example-context-xform-middleware [qp]
  (fn [query xformf context]
    (qp query xformf (-> (assoc context :extra-key? true)
                         (update :preprocessed (fn [preprocessed]
                                                 (fn [query]
                                                   (println "GOT PREPROCESSED QUERY!" query)
                                                   (preprocessed query))))))))

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
     (let [{:keys [xformf finish raise canceled-chan], :as context} (merge (default-context) context)]
       (assert (fn? xformf))
       (assert (fn? finish))
       (assert (fn? raise))
       (assert (some? canceled-chan))
       (let [result-chan (a/promise-chan)
             finish'     (fn [metadata reduced-rows]
                           (a/>!! result-chan (finish metadata reduced-rows)))
             raise'      (fn [e]
                           (a/>!! result-chan (raise e)))]
         ;; if `result-chan` gets closed before it gets a result, send a message to `canceled-chan`
         (a/go
           (let [[val port] (a/alts! [result-chan canceled-chan] :priority true)]
             (when (and (= val nil)
                        (= port result-chan))
               (a/>! canceled-chan :cancel))
             (a/close! result-chan)
             (a/close! canceled-chan)))
         (qp query xformf (merge context
                                 {:finish finish'
                                  :raise  raise'}))
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
