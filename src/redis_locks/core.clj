(ns redis-locks.core
  (:require [taoensso.carmine :as redis :refer [wcar]]))

(def DEFALT_POLL_INTERVAL 1000)
(def DEFAULT_KEY_TIMEOUT 10000)
(def redis-config {:pool {} :spec {:uri "redis://localhost:6379"}})
(defmacro in-redis [& body] `(redis/wcar redis-config ~@body))

(defn uuid [] (str (java.util.UUID/randomUUID)))
(defn- ->key [k] (if (= k :uuid) (uuid) k))
(defn- ->lock-key [s] (str "LOCK-" s))
(defn- ->fencing-token-key [s] (str "FT-" s))
(defn- compact [coll] (map (comp not nil) coll))
(defn- cast-redis-response [s]
  (cond
    (= s "t") true
    (= s "f") false 
    (re-matches #"\d+" s) (Integer/parseInt s)))

(defn acquire-lock [uuid duration]
  (cast-redis-response
    (in-redis
      (redis/lua "if redis.call('SET', _:lock-key, 1, 'NX', 'PX', _:lock-timeout) then
                    return redis.call('INCR', _:fencing-token)
                  else
                    return 'f' 
                  end"
                 {:lock-key (->lock-key uuid) :fencing-token (->fencing-token-key uuid)}
                 {:lock-timeout duration}))))

(defn release-lock [uuid fencing-token]
  (cast-redis-response
    (in-redis
      (redis/lua "if redis.call('EXISTS', _:lock-key) ~= 0 then
                    if redis.call('GET', _:fencing-token) == _:fencing-token-value then
                      redis.call('DEL', _:lock-key)
                      return 't'
                    else
                      return 'f'
                    end
                  else
                    return 'f'
                  end"
                 {:lock-key (->lock-key uuid) :fencing-token (->fencing-token-key uuid)}
                 {:fencing-token-value (str fencing-token)}))))

(defn still-locked? [uuid]
  (in-redis
    (redis/get (->lock-key uuid))))

(defn poll-lock [uuid timeout lock-timeout poll-interval]
  (let [end (+ (System/currentTimeMillis) timeout)]
      (loop [fencing-token (apply acquire-lock (compact [uuid lock-timeout]))]
        (if-not (zero? fencing-token)
          fencing-token 
          (do
            (Thread/sleep poll-interval)
            (if (and (not (nil? timeout)) (> (System/currentTimeMillis) end))
              false
              (recur (acquire-lock uuid lock-timeout))))))))

(defprotocol ILock
  (lock [this duration])
  (unlock [this])
  (locked? [this]))

(deftype RedisLock [uuid fencing-token lock-timeout poll-interval]
  ILock
  (lock [this timeout]
    (if-let [new-token (poll-lock uuid timeout lock-timeout poll-interval)]
      (reset! fencing-token new-token)
      false))
  (unlock [this]
    (let [released? (release-lock uuid @fencing-token)]
      (if (nil? released?)
        true 
        (reset! fencing-token released?))))
  (locked? [this]
    (not (nil? (still-locked? uuid)))))

(defn polling-lock 
  ([]
   (->RedisLock (uuid) (atom 0) DEFAULT_KEY_TIMEOUT DEFALT_POLL_INTERVAL))
  ([key-name]
   (->RedisLock (->key key-name) (atom 0) DEFAULT_KEY_TIMEOUT DEFALT_POLL_INTERVAL))
  ([key-name lock-timeout]
   (->RedisLock (->key key-name) (atom 0) lock-timeout DEFALT_POLL_INTERVAL))
  ([key-name lock-timeout poll-interval]
   (->RedisLock (->key key-name) (atom 0) lock-timeout poll-interval)))
