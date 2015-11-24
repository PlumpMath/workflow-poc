(ns workflow-engine-poc.master-worker
  (:require [clojure.core.async :refer [go-loop alts! <! >! >!! <!! timeout chan buffer thread] :exclude [reduce]]))


(defn worker [in-ch out-ch process-fn]
  (go-loop []
    (let [[v ch] (alts! [in-ch (timeout 1000)])]
      (if (= ch in-ch)
        (->> v
             process-fn
             (>! out-ch))
        (println "Timeout, retrying"))
      (if (not (= v :DONE))
        (recur)))))


(defn master [n]
  (let [work (chan 3)]
    (doto (Thread. (fn []
                     (loop [i 0]
                       (println "Sending work")
                       (>!! work [:work (str "THIS IS WORK ITEM_" i)])
                       (if (< i n)
                         (recur (inc i))
                         (>!! work :DONE)))))
      (.start))
    work))

(defn result []
  (let [out (chan)]
    (doto (Thread. (fn []
                     (let [r (<!! out)]
                       (if (= r :DONE)
                         (println "Finished processing")
                         (do
                           (println "Received: " r)
                           (recur))))))
      (.start))
    out))


(defn start-system []
  (worker (master 10) (result) (fn [v] (println v) v)))
