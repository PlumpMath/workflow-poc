(ns workflow-engine-poc.core
  (:require [clojure.core.async :refer [go <! >! >!! chan buffer thread]]
            [clojure.pprint :refer [pprint]]))


(def state (atom {}))
(def step1 (ref {}))
(def step2 (ref {}))

(defn worker-fn [input-ch output-ch task-fn update-state-fn ]
  (fn []
    (go (while true
          (let [input (<! input-ch)
                task-result (task-fn input)]
            (update-state-fn task-result) ;; possibly in a future
            (>! output-ch task-result))))))

(defn register-work [s worker-id new-value]
  (swap! s #(assoc % worker-id (conj (worker-id %) new-value))))

(defn init-workflow [init-ch]
  (let [processing-fn #(do
                         (Thread/sleep 3000)
                         %)
        first-ch (chan (buffer 3))
        second-ch (chan)
        first-worker (worker-fn first-ch second-ch processing-fn
                                #(register-work state :worker-1-1 %))
        third-ch (chan)
        second-worker (worker-fn second-ch third-ch processing-fn
                                 #(register-work state :worker-2 %))
        first-stage-worker-factory (fn [id] (worker-fn first-ch second-ch identity
                                                      #(register-work state id %)))]
    (thread ((worker-fn first-ch second-ch identity
                        #(register-work state :worker-2-1 %))))
    (thread ((first-stage-worker-factory :worker-3-1)))
    (thread (first-worker))
    (thread (second-worker))
    (go
      (while true
        (pprint @state)
        (let [init-val (<! init-ch)]
          (>! first-ch init-val))
        (let [result (<! third-ch)]
          (println ">>"  result))))))

(comment
  (let [bf (chan (buffer 3))]
    (>!! bf 1)
    (>!! bf 2)
    (go

      (println "BACK" (<! bf)))))

(def init-ch (chan (buffer 3)))

(defn input-event [init-ch value]
  (go  (>! init-ch value)))


(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
