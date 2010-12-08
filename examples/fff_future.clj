(ns fff-future
  "A \"Fastest Finger First\" implementation using reify and future."
  (:refer-clojure :exclude (future-cancel))
  (:use [se.raek.lcug.concurrency :only (wait notify-all future-get future-cancel)])
  (:import (java.util.concurrent Future
                                 TimeoutException
                                 CancellationException
                                 ExecutionException)))

(defn compare-value-and-set!
  [atom oldval newval]
  (= newval
     (swap! atom
            (fn [currentval]
              (if (= currentval oldval)
                newval
                oldval)))))

(defprotocol Triggerable
  "Something that someone may trigger."
  (trigger [this who]
    "Signals that 'who' has fired the trigger."))

(defprotocol Question
  "A question that someone may attempt to answer."
  (answer [this who what]
    "Signals that 'who' attempts to answer the question with 'what'."))

(defn create-trigger-contest
  "Creates a contest where the participants compete for being the first to
  fire the trigger. The returned object can be used as a Triggerable and a
  Future."
  []
  (let [state (atom [:pending])
        winner-announced (Object.)]
    (reify
      Object
      (toString [_]
        (pr-str @state))
      Triggerable
      (trigger [_ who]
        (locking winner-announced
          (when (compare-value-and-set! state [:pending] [:done who])
            (notify-all winner-announced))))
      Future
      (cancel [_ may-interrupt-if-running]
        (if-not may-interrupt-if-running
          false
          (locking winner-announced
            (reset! state [:cancelled])
            (notify-all winner-announced)
            true)))
      (get [_]
        (try
          (locking winner-announced
            (when (= @state [:pending])
              (wait winner-announced))
            (let [[state-type winner] @state]
              (case state-type
                :pending (throw (TimeoutException.))
                :done winner
                :cancelled (throw (CancellationException.)))))
          (catch InterruptedException e
            (throw e))
          (catch Throwable t
            (throw (ExecutionException. t)))))
      (get [_ timeout time-unit]
        (try
          (locking winner-announced
            (when (= @state [:pending])
              (wait winner-announced timeout time-unit))
            (let [[state-type winner] @state]
              (case state-type
                :pending (throw (TimeoutException.))
                :done winner
                :cancelled (throw (CancellationException.)))))
          (catch InterruptedException e
            (throw e))
          (catch Throwable t
            (throw (ExecutionException. t)))))
      (isCancelled [_]
        (= @state [:cancelled]))
      (isDone [_]
        (not= @state [:pending])))))
