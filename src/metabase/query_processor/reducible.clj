(ns metabase.query-processor.reducible
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log]
            [metabase
             [config :as config]
             [driver :as driver]
             [util :as u]]
            [metabase.query-processor.error-type :as error-type]))

(defn quit
  ([]
   (quit ::quit))
  ([response]
   (ex-info "<Quit early>" {::response response})))

(defn quit-response [result]
  (::response (ex-data result)))

(def query-timeout-ms
  "Maximum amount of time to wait for a running query to complete before throwing an Exception."
  ;; I don't know if these numbers make sense, but my thinking is we want to enable (somewhat) long-running queries on
  ;; prod but for test and dev purposes we want to fail faster because it usually means I broke something in the QP
  ;; code
  (cond
    config/is-prod? (u/minutes->ms 20)
    config/is-test? (u/seconds->ms 30)
    config/is-dev?  (u/minutes->ms 3)))

(defn default-finishf
  ([result] result)

  ([metadata reduced-rows]
   (-> (update metadata :data merge {:rows reduced-rows
                                     :cols (:cols metadata)})
       (dissoc :cols))))

(def default-xformf (constantly identity))

(defn default-rff [metadata]
  (let [row-count (volatile! 0)]
    (fn
      ([]
       [metadata []])

      ([[metadata rows]]
       {:pre [(map? metadata)]}
       [(assoc metadata :row_count @row-count)
        rows])

      ([[metadata rows] row]
       (vswap! row-count inc)
       [metadata (conj rows row)]))))

(defn transduce-rows [xformf {:keys [rff metadataf finishf raisef]} metadata reducible-rows]
  {:pre [(fn? xformf) (fn? rff) (fn? metadataf) (fn? finishf) (fn? raisef)]}
  (try
    (let [metadata (metadataf metadata)
          xform    (xformf metadata)
          rf       (rff metadata)
          rf       (xform rf)]
      (let [reduced-result     (transduce identity rf (rf) reducible-rows)
            [mta reduced-rows] reduced-result]
        (finishf mta reduced-rows)))
    (catch Throwable e
      (println "transduce-rows caught" (class e))
      (raisef e))))

(defn default-reducef [query xformf {:keys [executef raisef], :as context}]
  {:pre [(map? query) (fn? xformf) (ifn? executef)]}
  ;; TODO - consider whether to use `partial` here instead, or if it negatively affects readability too much
  (letfn [(respond* [metadata reducible-rows]
            (transduce-rows xformf context metadata reducible-rows))]
    (try
      (executef driver/*driver* query context respond*)
      (catch Throwable e
        (println "default-reducef caught" (class e))
        (raisef e)))))

;;
;; 1. Pivot calls `reducef` (reducef query xformf context)
;;
;; 2. `reducef` calls `executef` (usually `driver/execute-reducible-query`), which calls `respond`;
;;
;; 3. `respond` is a function provided by `executef` that reduces the results, then calls `finishf`.
;;
;;    (reducef query xformf context) [in `pivot`]
;;      |
;;      +-> (executef driver query context respond) [in `reducef`]
;;            |
;;            +-> (respond metadata reducible-rows) [in driver impl of `executef`]
;;                                            |
;;          (finishf metadata reduced-rows) <-+     [in `respond` fn provided by `reducef`]
;;
;; Example:
;;
;;    (defn reducef [query xformf context]
;;      (executef driver query context (fn respond [metadata rows]
;;                                       (finishf metadata (reduce rf rows)))))
(defn default-context
  "Default context used to execute a query. You can override these options as needed."
  []
  { ;; timeout to wait for query to complete.
   :timeout query-timeout-ms

   ;; Function used to execute the query. Has the signature. By default, `driver/execute-reducible-query`.
   ;;
   ;; (executef driver query context respond) -> (respond metadata reducible-rows)
   :executef driver/execute-reducible-query

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
   ;; (metadataf results-metadata) -> results-metadata
   :metadataf identity

   ;; combines the results metadata and reduced rows in to the final result returned by the QP.
   ;; One (and only one) of `finishf` or `raisef` will always be called.
   ;;
   ;; (finishf metadata reduced-rows)
   :finishf default-finishf

   ;; Called with any Exceptions thrown. One (and only one) of `finishf` or `raisef` will always be called.
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
   ;; `respond`. Should call `executef`
   ;;
   ;; (reducef query xformf context) -> (executef driver query respond context)
   :reducef default-reducef

   ;; sent a message if the query is canceled before finished, e.g. if the connection awaiting results is closed.
   :canceled-chan (a/promise-chan)})

(defn pivot [query xformf {reducef :reducef, :as context}]
  {:pre [(map? query) (fn? reducef)]}
  (reducef query xformf context))

(defn reducible-qp [qp]
  (fn qp*
    ([query]
     (qp* query nil))

    ([query context]
     (let [{:keys [xformf finishf raisef canceled-chan timeout], :as context} (merge (default-context) context)]
       (assert (fn? xformf))
       (assert (fn? finishf))
       (assert (fn? raisef))
       (assert (some? canceled-chan))
       (assert (number? timeout))
       (let [out-chan (a/promise-chan)]
         (letfn [(finishf*
                   ([result]
                    (u/prog1 (finishf result)
                      (a/>!! out-chan <>)))
                   ([metadata reduced-rows]
                    (finishf* (finishf metadata reduced-rows))))
                 (raisef* [e]
                   (finishf* (raisef e)))]
           ;; if result chan doesn't get a result/close by timeout, call `raisef`, which should eventually funnel the
           ;; exception thru result chan
           (a/go
             (let [[val port] (a/alts! [out-chan (a/timeout timeout)] :priority true)]
               (when (not= port out-chan)
                 (log/tracef "Query timed out after %d ms, raising timeout exception." timeout)
                 (raisef* (ex-info (format "Timed out after %s." (u/format-milliseconds timeout))
                                   {:status :timed-out
                                    :type   error-type/timed-out})))))
           ;; if `out-chan` gets closed before it gets a result, send a message to `canceled-chan`
           (a/go
             (let [[val port] (a/alts! [out-chan canceled-chan] :priority true)]
               #_(printf "port %s got %s\n" (if (= port out-chan) "out-chan" "canceled-chan") val #_(class val)) ; NOCOMMIT
               (when (and #_(= val nil)
                          (= port out-chan))
                 (println "out-chan closed" (if (nil? val) "early" "after getting response")) ; NOCOMMIT - switch this to log
                 (a/>! canceled-chan :cancel))
               (a/close! out-chan)
               (a/close! canceled-chan)))
           (qp query xformf (merge context
                                   {:finishf finishf*
                                    :raisef  raisef*})))
         out-chan)))))

(defn sync-qp
  "Returns a synchronous query processor function from an `async-query-processor`. QP function has the signatures

    (qp2 query) and (qp2 query rff)

  If a query is executed successfully, the results will be transduced using `rf` (the result of `(rff metadata)`)
  and transforms supplied by the middleware, and returned synchronously; if the query fails, and Exception will be
  thrown (also synchronously)."
  [qp]
  (fn qp*
    ([query]
     (qp* query nil))

    ([query context]
     (let [result (try
                    (first (a/alts!! [(or (qp query context)
                                          (throw (Exception. "EXPECTED ASYNC CHAN!")))
                                      (a/timeout query-timeout-ms)]))
                    (catch Throwable e e))
           result (or (quit-response result)
                      result)]
       (if (instance? Throwable result)
         (throw result)
         result)))))
