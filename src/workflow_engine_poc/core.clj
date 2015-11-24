(ns workflow-engine-poc.core
  (:require [clojure.core.async :refer [go <! >! >!! chan buffer thread]]
            [clojure.pprint :refer [pprint]]))


(def state (atom {}))
(def step0 (ref []))
(def step1 (ref []))
(def step2 (ref []))
(def step3 (ref []))
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

(defn display-state []
  (pprint {:step0 @step0
               :step1 @step1
               :step2 @step2
               :finished  @finished}))

(defn move-workflow-step [old-stage new-stage worker-id {payload :payload
                                                         owner :owner
                                                         owner-history :owner-history
                                                         :as value}]
  (let [new-val (-> value
                    (assoc :owner worker-id)
                    (assoc :owner-history (conj owner-history worker-id)))]
    (dosync
     (alter new-stage conj new-val)
     (alter old-stage (fn [s] (->> s
                                 (remove #(= (:payload %) payload))
                                 (into [])))))
    (comment (display-state))
    new-val))

(comment
  (init-workflow init-ch)

  (input-event init-ch "223")

  (move-workflow-step step0 step1 :worker-1-1 (first @step0))

  (move-workflow-step step1 step2 :worker-2 (first @step1))

  )

(defn init-workflow [init-ch]
  (let [processing-fn #(do
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
           (commute finished conj result)))))))

;; how about read in all keys from topology + :output-channel
;; and create a (chan) for each of them in dictionary..
(def sample-topology {:first {:step-fn #(do (Thread/sleep 3000)
                                      (println "Step 1 done")
                                      %)
                              :id :first ;;TODO remove duplication
                              :worker-num 3
                              :input :init-channel
                              :output :second
                              :step-state step1}
                      :second {:step-fn #(do (Thread/sleep 3000)
                                     (println "Step 2 done")
                                     %)
                               :id :second ;;TODO remove duplication
                               :worker-num 2
                               :input :first
                               :output :third
                               :step-state step2}
                      :third {:step-fn #(do (Thread/sleep 3000)
                                     (println "Step 3 done")
                                     %)
                              :id :third ;;TODO remove duplication
                              :worker-num 2
                              :input :second
                              :output :output-channel
                              :step-state step3}})

(defn topology->channel-dict [topology init-channel]
  (->
   (->> topology
        vals
        (mapcat (fn [{out :output in :input}] [in out]))
        (map (fn [ch k] [k ch]) (repeat (chan (buffer 5))) )
        (into {}))
   (assoc :init-channel init-channel)))

(comment
  (topology->channel-dict sample-topology init-ch))

(defn build-workflow-step [{step-fn :step-fn
                            step-id :id
                            worker-num :worker-num
                            input :input
                            output :output
                            step-state :step-state} topology chan-dict]
(println input)
  (let [prev-step-state (or (-> topology
                                input
                                :step-state) ;; TODO find better way to handle first state
                            :step0)]
    (map
     (fn [k]
       ( (worker-fn (chan-dict input) (chan-dict output) step-fn
                    #(do
                       (move-workflow-step prev-step-state  step-state k  %)))))
     (map #(keyword (str step-id %)) (range 1 (inc worker-num))))))

(comment
  (build-workflow-step
   (:first sample-topology)
   sample-topology
   (topology->channel-dict sample-topology init-ch)))

(defn find-exit-chan [topology chan-dict]
  (->> (vals topology)
       (filter (fn [{out-ch :output}] (= out-ch :output-channel)))
       first
       :output
       (chan-dict)))

(comment
  (find-exit-chan sample-topology (topology->channel-dict sample-topology init-ch)))

(defn build-workflow [topology input-channel]
  (let [chan-dict (topology->channel-dict topology init-ch)]
    (comment
      (->> (vals  topology)
           (map #(build-workflow-step % topology chan-dict))
           (into [])))
    (reduce
     (fn [w step]
       (into w
             (build-workflow-step step topology chan-dict)))
     []
     (vals topology))
    (find-exit-chan topology chan-dict)))

(comment
  (build-workflow sample-topology init-ch))

(comment
  (do
    (init-workflow init-ch)
    ;;run some events through
    (doseq [i (range 1 21)]
      (future (input-event init-ch (str  "TEST_" i)))))
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
