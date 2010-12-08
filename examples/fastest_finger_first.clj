(ns fastest-finger-first
  (:use [se.raek.lcug.concurrency :only (wait notify-all sleep)]))

(defn- try-to-win
  [winner there-is-a-winner who]
  (locking there-is-a-winner
    (notify-all there-is-a-winner)
    (compare-and-set! winner nil who)))

(defn- wait-for-winner
  ([winner there-is-a-winner]
     (locking there-is-a-winner
       (when-not @winner
         (wait there-is-a-winner)
         @winner)))
  ([winner there-is-a-winner timeout time-unit]
     (locking there-is-a-winner
       (when-not @winner
         (wait there-is-a-winner timeout time-unit)
         @winner))))

(defn create-contest []
  (let [winner (atom nil)
        there-is-a-winner (Object.)]
    [(partial try-to-win winner there-is-a-winner)
     (partial wait-for-winner winner there-is-a-winner)]))

(defn demo-contest []
  (let [[t w] (create-contest)]
    (doseq [i "ABCD"]
      (future (sleep 1 :seconds)
              (sleep (rand-int 2) :seconds)
              (if (t i)
                (printf "<%s> I was first! YAY!\n" i)
                (printf "<%s> I lost...\n" i))))
    (if-let [winner (w 2 :seconds)]
      (printf "<moderator> %s won.\n" winner)
      (printf "<moderator> no one won.\n"))))

(defn create-question [correct?]
  (let [[react wait] (create-contest)
        answer (fn [who what]
                 (when (correct? what)
                   (react)))]
    [answer wait]))