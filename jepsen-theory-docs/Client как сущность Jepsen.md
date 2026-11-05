### ***Hазначение Client в Jepsen***

Client - это интерфейс (протокол jepsen.client/Client), через который Jepsen выполняет операции над тестируемой системой. Он принимает операции от Generator, транслирует их в реальные вызовы к системе (в нашем случае - SQL-запросы к PostgreSQL), и возвращает результат (:ok, :fail или :info), который записывается в единый лог истории теста (history). Именно по этой истории впоследствии checker проверяет корректность поведения системы.
### ***Цикл жизни Client*** 

Протокол client/Client определяет следующие методы:

| **Метод**                 | **Назначение**                                                                                              |
| ------------------------- | ----------------------------------------------------------------------------------------------------------- |
| setup!                    | Разовая подготовка перед началом теста (например, создание таблиц). Вызывается один раз до старта нагрузки. |
| open!                     | Открытие клиента/соединения для конкретного узла кластера. Вызывается для каждого узла отдельно.            |
| invoke!                   | Выполнение операций — основной метод, где происходит трансляция op в SQL и обработка результата.            |
| close!                    | Закрытие соединения.                                                                                        |
| teardown!                 | Очистка после завершения теста.                                                                             |
| client/Reusable/reusable? | Определяет, можно ли переиспользовать уже открытый клиент между разными тестами без пересоздания.           |

### ***Пример реализации client***

```clj 
//patroni/jepsen/src/jepsen/patroni.clj

(defn open-conn
  "Given a JDBC connection spec, opens a new connection unless one already
  exists. JDBC represents open connections as a map with a :connection key.
  Won't open if a connection is already open."
  [spec]
  (if (:connection spec)
    spec
    (j/add-connection spec (j/get-connection spec))))

(defn close-conn
  "Given a spec with JDBC connection, closes connection and returns the spec w/o connection."
  [spec]
  (when-let [conn (:connection spec)]
    (.close conn))
  {:classname   (:classname spec)
   :subprotocol (:subprotocol spec)
   :subname     (:subname spec)
   :user        (:user spec)
   :password    (:password spec)})

(defmacro with-conn
  "This macro takes that atom and binds a connection for the duration of
  its body, automatically reconnecting on any
  exception."
  [[conn-sym conn-atom] & body]
  `(let [~conn-sym (locking ~conn-atom
                     (swap! ~conn-atom open-conn))]
     (try
       ~@body
       (catch Throwable t#
         (locking ~conn-atom
           (swap! ~conn-atom (comp open-conn close-conn)))
         (throw t#)))))

(defn conn-spec
  "Return postgresql connection spec for given node name"
  [node]
  {:classname   "org.postgresql.Driver"
   :subprotocol "postgresql"
   :subname     (str "//" (name node) ":5432/postgres?prepareThreshold=0")
   :user        "postgres"
   :password    "postgres"})

(defn noop-client
  "Noop client"
  []
  (reify client/Client
    (setup! [_ test]
      (info "noop-client setup"))
    (invoke! [this test op]
      (assoc op :type :info, :error "noop"))
    (close! [_ test])
    (teardown! [_ test] (info "teardown"))
    client/Reusable
    (reusable? [_ test] true)))




(defn pg-client
  "PostgreSQL client"
  [conn]
  (reify client/Client
    (setup! [_ test]
      (info "pg-client setup"))
    (open! [_ test node]
      (let [conn (atom (conn-spec node))]
        (cond (string/includes? (name node) "patroni")
              (pg-client conn)
              true
              (noop-client))))
    (invoke! [this test op]
      (try
          (timeout 5000 (assoc op :type :info, :error "timeout")
            (with-conn [c conn]
              (case (:f op)
                :read (assoc op :type :ok,
                                :value (->> (j/query c ["select value from set for update"]
                                                     {:row-fn :value})
                                            (vec)
                                            (set)))
                :add (do (j/execute! c [(str "insert into set values ("
                                                (get op :value) ")")])
                            (assoc op :type :ok)))))
        (catch Throwable t#
          (let [m# (.getMessage t#)]
            (cond (re-find #"ERROR: cannot execute .* in a read-only transaction" m#)
                  (assoc op :type :info, :error "read-only")
                  true
                  (assoc op :type :info, :error m#))))))
    (close! [_ test] (close-conn conn))
    (teardown! [_ test])
    client/Reusable
    (reusable? [_ test] true)))

```

### ***Преобразование операции в sql запрос***

Преобразование происходит внутри invoke!, через case по полю (:f op):

```clj
   (invoke! [this test op]
      (try
          (timeout 5000 (assoc op :type :info, :error "timeout")
            (with-conn [c conn]
              (case (:f op)
                :read (assoc op :type :ok,
                                :value (->> (j/query c ["select value from set for update"]
                                                     {:row-fn :value})
                                            (vec)
                                            (set)))
                :add (do (j/execute! c [(str "insert into set values ("
                                                (get op :value) ")")])
                            (assoc op :type :ok)))))
        (catch Throwable t#
          (let [m# (.getMessage t#)]
            (cond (re-find #"ERROR: cannot execute .* in a read-only transaction" m#)
                  (assoc op :type :info, :error "read-only")
                  true
                  (assoc op :type :info, :error m#))))))
```

- :read → SELECT value FROM set FOR UPDATE - читает всё множество с блокировкой строк, результат оборачивается в Clojure-множество (set).
- :add → INSERT INTO set VALUES (N) - добавляет одно значение; N берётся из атома-счётчика register (генерируется вне Client, функцией-генератором a).

invoke! не решает, какое значение писать или читать - эта логика приходит извне, в op. Задача invoke! - только транслировать уже готовую операцию в SQL и корректно классифицировать исход.

### ***Обработка ошибок***

В Jepsen у операции может быть три типа исхода:

| **:type** | **Значение**                                         | **Что это говорит checker'у**         |
| --------- | ---------------------------------------------------- | ------------------------------------- |
| :ok       | Операция точно выполнилась успешно                   | Эффект точно есть                     |
| :fail     | Операция точно не выполнилась (подтверждённый отказ) | Эффекта точно нет                     |
| :info     | Исход неизвестен                                     | Checker должен допускать оба варианта |
В invoke! есть три места, где формируется исход операции:

1. Таймаут. (timeout 5000 (assoc op :type :info, :error "timeout") ...) - если операция не укладывается в 5 секунд, сразу возвращается :info с "timeout", ещё до попытки выполнить SQL или уже во время его выполнения - в обоих случаях мы не знаем, применилась ли операция на самом деле, поэтому это :info, а не :fail.
2. Успех. Если j/query/j/execute! отработали без исключений - выполнение доходит до (assoc op :type :ok ...).
3. Исключение. Любая ошибка (deadlock, serialization failure, разрыв соединения, попытка писать на реплику и т.п.) перехватывается try/catch:

```clj
(catch Throwable t#
  (let [m# (.getMessage t#)]
    (cond (re-find #"ERROR: cannot execute .* in a read-only transaction" m#)
          (assoc op :type :info, :error "read-only")
          true
          (assoc op :type :info, :error m#))))
```
### ***Роль with-conn***

```clj
(defmacro with-conn

"This macro takes that atom and binds a connection for the duration of

its body, automatically reconnecting on any

exception."

[[conn-sym conn-atom] & body]

`(let [~conn-sym (locking ~conn-atom

(swap! ~conn-atom open-conn))]

(try

~@body

(catch Throwable t#

(locking ~conn-atom

(swap! ~conn-atom (comp open-conn close-conn)))

(throw t#)))))
```

Отдельно стоит различать два уровня обработки ошибок:

 - with-conn - это уровень технического восстановления соединения. При любом исключении внутри body он закрывает и переоткрывает соединение ((comp open-conn close-conn)), но не решает, какой :type должна получить операция — он просто пробрасывает исключение дальше.
- try/catch в invoke! - это уровень семантической классификации результата: именно здесь исключение превращается в конкретную запись {:type :info, :error ...}, которая попадёт в history.
### ***Схема прохождения одной операции:***

![[Pasted image 20260814145903.png]]


*Подробнее про работу связки Client и Generator будет в файле Generator.*

