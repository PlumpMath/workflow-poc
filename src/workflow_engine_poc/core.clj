(ns workflow-engine-poc.core
  (:require [clojure.core.async :refer [go <! >! >!! chan buffer thread]]
            [clojure.pprint :refer [pprint]]))


(def state (atom {}))
(def step0 (ref {}))
(def step1 (ref {}))
(def step2 (ref {}))
(def finished (ref []))
(def display-state? (atom true))

(defn worker-fn [input-ch output-ch task-fn update-state-fn ]
  (fn []
    (go (while true
          (let [input (<! input-ch)
                task-result (task-fn input)]
            (update-state-fn task-result) ;; possibly in a future
            (>! output-ch task-result))))))

(defn register-work [s worker-id new-value]
  (swap! s #(assoc % worker-id (conj (worker-id %) new-value)))
  )


(defn move-workflow-step [old-stage new-stage worker-id value]
  (dosync
   (alter new-stage assoc worker-id value)
   (alter old-stage dissoc worker-id)))

(comment
  (move-workflow-step step0 step1 :worker-1-1 "TEST_a"))

(defn init-workflow [init-ch]
  (let [processing-fn #(do
                         (println %)
                         (Thread/sleep 3000)
                         %)
        first-ch (chan (buffer 3))
        second-ch (chan)

        third-ch (chan)
        second-worker (worker-fn second-ch third-ch processing-fn
                                 #(do (register-work state :worker-2 %)
                                      (move-workflow-step step1 step2 :worker-2 %)))
        first-stage-worker-factory (fn [id] (worker-fn first-ch second-ch identity
                                                      #(do
                                                         (register-work state id %)
                                                         (comment)
                                                         (move-workflow-step step0 step1 id %)
                                                         )))]

    ;;TODO this should be in a loop over [:worker-1-1 :worker-1-2 :worker-1-3]
    (thread ((first-stage-worker-factory :worker-1-1)))
    (thread ((first-stage-worker-factory :worker-1-2)))
    (thread ((first-stage-worker-factory :worker-1-3)))

    (comment
      (thread (while @display-state?
                (Thread/sleep 1000)
                (println "Current State ")
                (pprint @state)
                (println " ")
                (pprint [@step0 @step1 @step2 @finished]))))

    (thread (second-worker))
    (go
      (while true
        (comment)
        (pprint @state)
        (let [init-val (<! init-ch)]
          (dosync
           (alter step0 assoc :workflow init-val))

          (>! first-ch init-val))
        (let [result (<! third-ch)]
          (dosync
           (commute finished conj result))
          (println ">>"  result))))))

(comment
  first-worker (worker-fn first-ch second-ch processing-fn
                                #(register-work state :worker-1-1 %))

  (thread ((worker-fn first-ch second-ch identity
                          #(register-work state :worker-2-1 %))))

  (thread (first-worker)))

(comment
  (let [bf (chan (buffer 3))]
    (>!! bf 1)
    (>!! bf 2)
    (go

      (println "BACK" (<! bf)))))

(def init-ch (chan (buffer 3)))


(defn input-event [init-ch value]
  (println "event:" value)
  (go  (>! init-ch value)))


(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))
