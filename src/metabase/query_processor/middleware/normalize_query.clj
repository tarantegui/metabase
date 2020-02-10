(ns metabase.query-processor.middleware.normalize-query
  "Middleware that converts a query into a normalized, canonical form."
  (:require [metabase.mbql.normalize :as normalize]
            [metabase.query-processor.error-type :as error-type]
            [metabase.util.i18n :refer [tru]]))

(defn normalize
  "Middleware that converts a query into a normalized, canonical form, including things like converting all identifiers
  into standard `lisp-case` ones, removing/rewriting legacy clauses, removing empty ones, etc. This is done to
  simplifiy the logic in the QP steps following this."
  [qp]
  (fn [query xformf context]
    (qp
     (try
       (normalize/normalize query)
       (catch Throwable e
         (throw (ex-info (tru "Invalid query.")
                  {:type  error-type/invalid-query
                   :query query}
                  e))))
     xformf
     context)))
