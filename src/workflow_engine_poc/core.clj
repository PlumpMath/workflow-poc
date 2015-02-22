(ns workflow-engine-poc.core
  (:require [clojure.core.async :refer [go <! >! >!! chan buffer thread]]
            [clojure.pprint :refer [pprint]]))


(def state (atom {}))
(def step0 (ref []))
(def step1 (ref []))
(def step2 (ref []))
(def finished (ref []))
(def display-state? (atom true))

(defn worker-fn [input-ch output-ch task-fn update-state-fn ]
  (fn []
    (go (while true
          (let [input (<! input-ch)
                task-result (task-fn input)
                updated-task (update-state-fn task-result)]
            ;; possibly in a future
            (->> (task-fn input)
                 (update-state-fn)
                 (>! output-ch)))))))

(defn register-work [s worker-id new-value]
  (swap! s #(assoc % worker-id (conj (worker-id %) new-value)))
  )


(defn move-workflow-step [old-stage new-stage worker-id {payload :payload
                                                         owner :owner
                                                         owner-history :owner-history
                                                         :as value}]
  (comment
    (pprint [@step0 @step1 @step2 @finished])
    (pprint value))
  (let [new-val (-> value
                    (assoc :owner worker-id)
                    (assoc :owner-history (conj owner-history worker-id)))]
    (dosync
     (alter new-stage conj new-val)
     (alter old-stage (fn [s] (->> s
                                 (remove #(= (:payload %) payload))
                                 (into [])))))
    (comment
      (pprint {:step0 @step0
               :step1 @step1
               :step2 @step2
               :finished  @finished}))
    new-val))

(comment
  (init-workflow init-ch)

  (input-event init-ch "223")

  (move-workflow-step step0 step1 :worker-1-1 (first @step0))

  (move-workflow-step step1 step2 :worker-2 (first @step1))

  )

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
        (comment
          (pprint @state))
        (let [init-payload (<! init-ch)
              init-val {:payload init-payload :owner-history []}]
          (dosync
           (alter step0 conj init-val))

          (>! first-ch init-val))
        (let [result (<! third-ch)]
          (dosync
           (commute finished conj result))
          (println ">>"  result))))))

(comment
  (init-workflow init-ch)
  ;;run some events through
  (doseq [i (range 1 21)] (future (input-event init-ch (str  "TEST_" i))))
  )


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
