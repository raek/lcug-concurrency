(ns se.raek.lcug.concurrency
  (:use [clojure.contrib.import-static :only (import-static)])
  (:import (java.util.concurrent Future Executor ExecutorService
                                 Executors)))

;;; Time related functions

(import-static java.util.concurrent.TimeUnit
               DAYS HOURS MINUTES SECONDS
               MILLISECONDS MICROSECONDS NANOSECONDS)

(defn- lookup-time-unit
  "Takes a java.util.concurrent.TimeUnit value or a keyword with the
  corresponding lowercase name and returns a TimeUnit value."
  [time-unit]
  (get {:days         DAYS
        :hours        HOURS
        :minutes      MINUTES
        :seconds      SECONDS,
        :milliseconds MILLISECONDS
        :microseconds MICROSECONDS
        :nanoseconds  NANOSECONDS}
       time-unit
       time-unit))

(defn- millis-and-nanos
  "Takes a time and a java.util.concurrent.TimeUnit and returns a
  vector of the time split into milliseconds and nanoseconds."
  [time time-unit]
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
  "Makes the current thread sleep for the given amount of time. Unlike
  its underlying method, this function returns immediately when time
  is negative. Throws an InterruptedException and clears the
  interrupted status if the thread is interrupted by another thread
  while sleeping.

  See Also:
    java.lang.Thread.sleep"
  [time time-unit]
  (let [time-unit (lookup-time-unit time-unit)
        [millis nanos] (millis-and-nanos time time-unit)]
    (cond (or (neg? millis)
              (neg? nanos))
          nil
          (zero? nanos)
          (Thread/sleep millis)
          :else
          (Thread/sleep millis nanos))))

;;; Wrapper functions for the monitor methods of Object
;;
;; Useful resource:
;;   Using wait(), notify() and notifyAll() in Java: common problems and mistakes
;;   http://www.javamex.com/tutorials/synchronization_wait_notify_3.shtml

(defn wait
  "Makes the current thread wait until another thread invokes notify
  on the given object. Unlike its underlying method, this function
  returns immediately when timeout is negative or zero. Throws an
  InterruptedException and clears the interrupted status if the thread
  is interrupted by another thread while waiting. Throws an
  IllegalMonitorStateException if the current thread is not in the
  monitor of o (e.g. with clojure.core/locking).

  See Also:
    clojure.core/locking
    java.lang.Object.wait"
  ([^Object o]
     (.wait o))
  ([^Object o timeout time-unit]
     (let [time-unit (lookup-time-unit time-unit)
           [millis nanos] (millis-and-nanos timeout time-unit)]
       (cond (or (and (zero? millis) (zero? nanos))
                 (neg? millis) (neg? nanos))
             nil,
             (zero? nanos) (.wait o millis),
             :else (.wait o millis nanos)))))

(defn notify
  "Wakes up one thread that is waiting on the monitor of object
  o. Throws an IllegalMonitorStateException if the current thread is
  not in the monitor of o (e.g. with clojure.core/locking).

  See Also:
    clojure.core/locking
    java.lang.Object.notify"
  [^Object o]
  (.notify o))

(defn notify-all
  "Wakes up all threads that are waiting on the monitor of object
  o. Throws an IllegalMonitorStateException if the current thread is
  not in the monitor of o (e.g. with clojure.core/locking).

  See Also:
    clojure.core/locking
    java.lang.Object.notifyAll"
  [^Object o]
  (.notifyAll o))

;;; Supplementary future functions
;;
;; See Also:
;;   clojure.core/future
;;   clojure.core/future-call
;;   clojure.core/future-cancel
;;   clojure.core/future-cancelled?
;;   clojure.core/future-done?
;;   java.util.concurrent.Future.cancel
;;   java.util.concurrent.Future.get
;;   java.util.concurrent.Future.isCancelled
;;   java.util.concurrent.Future.isDone

(defn future-get
  "Blocks until the computation of the future is finished (like
  deref/@), or until a timeout occurs (if given).

  See Also:
    clojure.core/deref
    java.util.concurrent.Future.get"
  ([^Future f]
     (.get f))
  ([^Future f timeout time-unit]
     (.get f timeout (lookup-time-unit time-unit))))

;;; Thread analogies of the future functions

(defn thread?
  "Returns true if x is a thread."
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

;;; Convenience wrappers for threads

(defn current-thread
  "Returns the thread object of the thread that invokes this
  function.

  See Also:
    java.lang.Thread.currentThread"
  []
  (Thread/currentThread))

(defn join
  "Waits for the thread to terminate. Unlike its underlying method,
  this function returns immediately when timeout is negative or
  zero. Throws an InterruptedException and clears the interrupted
  status if the thread is interrupted by another thread while waiting.

  See Also:
    java.lang.Thread.join"
  ([^Thread t]
     (.join t))
  ([^Thread t timeout time-unit]
     (let [time-unit (lookup-time-unit time-unit)
           [millis nanos] (millis-and-nanos timeout)]
       (cond (or (and (zero? millis) (zero? nanos))
                 (neg? millis) (neg? nanos))
             nil,
             (zero? nanos) (.join t millis),
             :else (.join t millis nanos)))))

;;; Thread interruption

(defn interrupted?
  "Returns the interrupted status of thread t.

  See Also:
    java.lang.Thread.isInterrupted"
  [^Thread t]
  (.isInterrupted t))

(defn interrupt
  "Interrupts thread t.

  If the thread is blocking in a invokation of
  Object.wait, Thread.join or Thread.sleep the interrupted status of
  the thread will be cleared and an InterruptedException will be
  thrown from that call.

  If the thread is blocking in an I/O operation on a interrubtible
  channel, the channel will be closed, the interrupted status of the
  thread set and an ClosedByInterruptException will be thrown from
  that call.

  If the thread is blocking in a selector, the interrupted status of
  the thread will be set and then the thread will return immediately
  from that call.

  Otherwise, the only effect of this function is that interrupted
  status of the thread will be set, which the thread will have to
  check itself.

  See Also:
    java.lang.Thread.interrup"
  [^Thread t]
  (.interrupt t))

(defn clear-interrupted-status
  "Clears the interrupted status of the current thread (the thread
  where this function is invoked) and returns the interrupted status
  is had before.

  See Also:
    java.lang.Thread.interrupted"
  []
  (Thread/interrupted))

;;; Runnables

(defn run
  [^Runnable f]
  (.run f))

;;; Callables

(defn call
  [^Callable f]
  (.call f))

;;; Executors

(defn execute
  [^Executor exe ^Runnable f]
  (.execute exe f))

;; Executor Services

(defn await-termination
  [^ExecutorService exe timeout time-unit]
  (.awaitTermination exe timeout (lookup-time-unit time-unit)))

(defn invoke-all
  ([^ExecutorService exe fs]
     (.invokeAll exe fs))
  ([^ExecutorService exe fs timeout time-unit]
     (.invokeAll exe fs timeout (lookup-time-unit time-unit))))

(defn invoke-any
  ([^ExecutorService exe fs]
     (.invokeAny exe fs))
  ([^ExecutorService exe fs timeout time-unit]
     (.invokeAny exe fs timeout (lookup-time-unit time-unit))))

(defn shutdown?
  [^ExecutorService exe]
  (.isShutdown exe))

(defn terminated?
  [^ExecutorService exe]
  (.isShutdown exe))

(defn shutdown
  [^ExecutorService exe]
  (.shutdown exe))

(defn shutdown-now
  [^ExecutorService exe]
  (.shutdownNow exe))

(defn submit
  ([^ExecutorService exe ^Runnable f]
     (.submit exe f))
  ([^ExecutorService exe ^Runnable f result]
     (.submit exe f result)))

;;; Executor Service Constructors

(defn create-cached-thread-pool
  []
  (Executors/newCachedThreadPool))

(defn create-fixed-thread-pool
  [n]
  (Executors/newFixedThreadPool n))

(defn create-single-thread-executor
  [n]
  (Executors/newSingleThreadExecutor))
