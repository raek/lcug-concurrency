(ns se.raek.lcug.concurrency
  (:use [clojure.contrib.import-static :only (import-static)])
  (:import java.util.concurrent.Future))

;;; Time related functions (here be redundancy)

(import-static java.util.concurrent.TimeUnit
               DAYS HOURS MINUTES SECONDS
               MILLISECONDS MICROSECONDS NANOSECONDS)

(defn- lookup-time-unit [time-unit]
  (get {:days DAYS, :hours HOURS, :minutes MINUTES, :seconds SECONDS,
        :milliseconds MILLISECONDS, :microseconds MICROSECONDS,
        :nanoseconds NANOSECONDS}
       time-unit
       time-unit))

(defn- millis-and-nanos [time time-unit]
  (condp = time-unit
      DAYS         [(* 24 60 60 1000 time) 0]
      HOURS        [(* 60 60 1000 time) 0]
      MINUTES      [(* 60 1000 time) 0]
      SECONDS      [(* 1000 time) 0]
      MILLISECONDS [time 0]
      MICROSECONDS (let [millis (quot time 1000)
                         nanos  (* (rem time 1000) 1000)]
                     [millis nanos])
      NANOSECONDS  (let [millis (quot time 1000000)
                         nanos  (rem time 1000000)]
                     [millis nanos])
      (throw (IllegalArgumentException.
              (str "Illegal time unit: " time-unit)))))

(defn sleep
  ([timeout-ms]
     (when-not (neg? timeout-ms)
       (Thread/sleep timeout-ms)))
  ([timeout time-unit]
     (let [time-unit (lookup-time-unit time-unit)
           [millis nanos] (millis-and-nanos timeout time-unit)]
       (cond (or (neg? millis)
                 (neg? nanos))
             nil
             (zero? nanos)
             (Thread/sleep millis)
             :else
             (Thread/sleep millis nanos)))))

;;; Wrapper functions for the monitor methods of Object (here be redundancy)
;;
;; Useful resource:
;;   Using wait(), notify() and notifyAll() in Java: common problems and mistakes
;;   http://www.javamex.com/tutorials/synchronization_wait_notify_3.shtml

(defn wait
  ([^Object o]
     (.wait o))
  ([^Object o timeout-ms]
     (.wait o timeout-ms))
  ([^Object o timeout time-unit]
     (let [time-unit (lookup-time-unit time-unit)
           [millis nanos] (millis-and-nanos timeout)]
       (cond (or (neg? millis)
                 (neg? nanos)
                 (and (zero? millis)
                      (zero? nanos)))
             nil
             (zero? nanos)
             (.wait o millis)
             :else
             (.wait o millis nanos)))))

(defn notify
  [^Object o]
  (.notify o))

(defn notify-all
  [^Object o]
  (.notifyAll o))

;;; Supplementary future functions

(defn future-get
  ([^Future f]
     (.get f))
  ([^Future f timeout-ms]
     (.get f timeout-ms MILLISECONDS))
  ([^Future f timeout time-unit]
     (.get f timeout (lookup-time-unit time-unit))))

;;; Thread analogies of the future functions

(defn thread?
  "Returns true if x is a thread"
  [x]
  (instance? Thread x))

(defn thread-call
  "Takes a function of no args and yields a thread object that will
  invoke the function in another thread."
  [^Runnable f]
  (doto (Thread. f)
    .start))

(defmacro thread
  "Takes a body of expressions and yields a thread object that will
  invoke the body in another thread."
  [& body]
  `(thread-call (fn [] ~@body)))
