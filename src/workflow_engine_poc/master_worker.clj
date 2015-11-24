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
      (recur))))


(defn master []
  (let [work (chan)]
    (doto (Thread. (fn []
                     (Thread/sleep 100)
                     (println "Sending work")
                     (>!! work [:work "THIS IS WORK"])))
      (.start))
    work))

(defn result []
  (let [out (chan)]
    (doto (Thread. (fn []
                     (Thread/sleep 100)
                     (println "Received: " (<!! out))))
      (.start))
    out))


(defn start-system []
  (worker (master) (result) println))
