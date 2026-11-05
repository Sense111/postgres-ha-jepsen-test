### ***Назначение Jepsen в проекте Patroni***

В Patroni Jepsen используется не как модульный/юнит-тест, а как отдельный тест устойчивости: поднимается 3-узловой кластер Patroni + PostgreSQL поверх etcd, к нему одновременно применяется нагрузка на запись/чтение и набор сетевых/процессных отказов (разделения сети, kill postgres/patroni, принудительный switchover), после чего по логу операций проверяется, не потерял ли кластер подтверждённые и не появились ли лишние записи. То есть тест отвечает на вопрос: сохраняет ли Patroni целостность данных PostgreSQL при автоматических failover/switchover под сетевыми и процессными сбоями.

### ***Компоненты теста*** 

Структура Jepsen внутри репозитория patroni:
![[Pasted image 20260810171514.png]]

| **Файл / каталог**                  | **Назначение**                                                                                                                                       |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Dockerfile                          | Многоступенчатая сборка (base → etcd / patroni / jepsen): готовит SSH, iptables, локали, ключи и три отдельных образа-роли для контейнеров кластера. |
| docker-compose.yml                  | Топология стенда: сервисы test (jepsen-контроллер), etcd1..3, patroni1..3, все в одной docker-сети jepsen.                                           |
| runit/etcd/run, runit/patroni/run   | Скрипты запуска сервисов внутри контейнеров (генерация patroni.yml, запуск etcd/patroni под runsvdir).                                               |
| jepsen/project.clj                  | Файл для сборки Jepsen-теста (содержит зависимости)                                                                                                  |
| jepsen/run.sh                       | Оркестрирующий bash-скрипт: подготовка SSH, ожидание лидера, создание таблицы, запуск lein test.                                                     |
| jepsen/src/jepsen/patroni.clj       | Основной Clojure-namespace теста: клиент, checker, nemesis, генератор нагрузки, сборка test-map.                                                     |
| jepsen/test/jepsen/patroni_test.clj | Точка входа deftest, запускающая jepsen/run! с готовым test-map.                                                                                     |

### ***Клиент***

```clj
//patroni/jepsen/jepsen/src/jepsen/patroni.clj
(defn pg-client
  "PostgreSQL client"
  [conn]
  (reify client/Client
    (setup! [_ test] ...)
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

клиент выполняет либо :add (вставку целого числа в таблицу set), либо :read (SELECT ... FOR UPDATE - чтение всего множества с блокировкой строк). Значение для :add берётся из монотонно растущего атома register - это гарантирует, что каждое значение уникально и его легко сверить. *P.S. атом - тип данных (контейнер)  для управления изменяемым cостоянием в многопоточной среде. По умолчанию все обычные структуры данных в Clojure  (списки, карты, векторы, числа) неизменяемы (immutable).  Когда вам всё-таки нужно хранить данные, которые меняются  во времени (например, счетчик, текущее соединение с БД или состояние приложения), эти данные заворачиваются в атом.*

Jepsen открывает клиент на каждом узле кластера (:nodes = patroni1..3 + etcd1..3), но реальные SQL-операции выполняются только на узлах, чьё имя содержит "patroni"; для etcd* подставляется noop-client. Соединение при любой ошибке пересоздаётся (with-conn/open-conn/close-conn), а попытка писать на реплику (read-only) распознаётся отдельно и размечается как :error "read-only".

Сами операции, которые Jepsen подставляет в поток :invoke, порождаются двумя отдельными функциями-генераторами (не путать с клиентом pg-client, который их потом исполняет):

```clj
//patroni/jepsen/jepsen/src/jepsen/patroni.clj
(def register (atom 0))

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn a [_ _] {:type :invoke, :f :add, :value (swap! register (fn [current-state] (+ current-state 1)))})
```

| **Функция** | **Что** **генерирует**                         | **Откуда берётся :value**                                                                                     |
| ----------- | ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| r           | операцию {:type :invoke, :f :read, :value nil} | значение не нужно - читается всё множество целиком, :value заполнится в invoke! клиента результатом SELECT    |
| a           | операцию {:type :invoke, :f :add, :value N}    | N - результат swap! над общим для всех потоков атомом register, который на каждый вызов инкрементируется на 1 |

Именно r и a (а не pg-client) - это генераторы: gen/stagger, gen/nemesis, gen/time-limit навешаны именно на них. Каждый вызов a атомарно берёт следующее целое число из общего счётчика register, поэтому:
значения :add гарантированно уникальны и монотонно возрастают (1, 2, 3, …) вне зависимости от того, сколько параллельных клиентских потоков одновременно работает - это и позволяет checker'у patroni-set детерминированно сверять множества attempts/adds/final-read без коллизий значений;
register - общий для всего процесса JVM атом, то есть нумерация сквозная по всему тесту независимо от того, на какой узел конкретная операция в итоге попадёт;
r не несёт полезной нагрузки на входе - её :value игнорируется клиентом при :f :read (:read в invoke! заполняет :value результатом запроса, входной nil не используется).
Сгенерированная функциями r/a «сырая» операция ({:type :invoke, :f :add, :value N}) затем передаётся Jepsen-ядром в client/invoke! конкретного pg-client/noop-client - только там она превращается в реальный SQL (INSERT/SELECT) и получает финальный :type (:ok/:info/:fail).
### ***Чекер***

```clj
//patroni/jepsen/jepsen/src/jepsen/patroni.clj
(def patroni-set
  "Given a set of :add operations followed by a final :read, verifies that
  every successfully added element is present in the read, and that the read
  contains only elements for which an add was attempted."
  (reify checker/Checker
    (check [this test history opts]
      (let [attempts (->> history
                          (r/filter op/invoke?)
                          (r/filter #(= :add (:f %)))
                          (r/map :value)
                          (into #{}))
            adds (->> history
                      (r/filter op/ok?)
                      (r/filter #(= :add (:f %)))
                      (r/map :value)
                      (into #{}))
            final-read (->> history
                          (r/filter op/ok?)
                          (r/filter #(= :read (:f %)))
                          (r/map :value)
                          (reduce (fn [_ x] x) nil))]
        (if-not final-read
          {:valid? false
           :error  "Set was never read"}

          (let [; The OK set is every read value which we tried to add
                ok          (set/intersection final-read attempts)

                ; Unexpected records are those we *never* attempted.
                unexpected  (set/difference final-read attempts)

                ; Lost records are those we definitely added but weren't read
                lost        (set/difference adds final-read)

                ; Recovered records are those where we didn't know if the add
                ; succeeded or not, but we found them in the final set.
                recovered   (set/difference ok adds)]

            {:valid?          (and (empty? lost) (empty? unexpected))
             :ok              (util/integer-interval-set-str ok)
             :lost            (util/integer-interval-set-str lost)
             :unexpected      (util/integer-interval-set-str unexpected)
             :recovered       (util/integer-interval-set-str recovered)
             :ok-frac         (util/fraction (count ok) (count attempts))
             :unexpected-frac (util/fraction (count unexpected) (count attempts))
             :lost-frac       (util/fraction (count lost) (count attempts))
             :recovered-frac  (util/fraction (count recovered) (count attempts))}))))))
```

| **Множество** | **Как вычисляется**                                                 | **Что означает**                                                                                                                        |
| ------------- | ------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| attempts      | все значения, для которых была  попытка :add (независимо от исхода) | всё, что клиент когда-либо пытался записать                                                                                             |
| adds          | значения, для которых :add вернул :ok  (подтверждённая запись)      | то, что кластер явно подтвердил как записанное                                                                                          |
| final-read    | значение последней успешной операции :read                          | «истина» на момент окончания теста                                                                                                      |
| ok            | пересечение final-read и  attempts                                  | значения, реально найденные при чтении                                                                                                  |
| unexpected    | final-read − attempts                                               | значения в БД, которых клиент вообще не писал - признак порчи/дублирования данных                                                       |
| lost          | adds − final-read                                                   | подтверждённая запись пропала - потеря данных при failover, нарушение durability                                                        |
| recovered     | ok − adds                                                           | запись, чей исход был неизвестен клиенту (таймаут/ошибка), но фактически попала в БД - нормальная ситуация при сетевых сбоях, не ошибка |

:valid? = true, только если нет lost и нет unexpected. Таким образом тест проверяет две гарантии: отсутствие потери подтверждённых записей (durability) и отсутствие «лишних»/сфабрикованных значений (целостность). Это не тест сериализуемости порядка операций (в отличие от, например, официальных Jepsen-анализов PostgreSQL с elle/list-append) - здесь проверяется только итоговый состав множества после отдельной read-фазы, а не порядок чтений в реальном времени.

### ***Нагрузки и сценарий выполнения (Генератор)***

```clj
//patroni/jepsen/jepsen/src/jepsen/patroni.clj
:generator (gen/phases
             (->> a
                  (gen/stagger 1/50)
                  (gen/nemesis
                    (fn [] (map gen/once
                                 [{:type :info, :f (rand-nth nemesis-starts)}
                                  {:type :info, :f (rand-nth nemesis-starts)}
                                  {:type :sleep, :value 60}
                                  {:type :info, :f :stop}
                                  {:type :sleep, :value 60}])))
                  (gen/time-limit 7200))
             (->> r
                  (gen/stagger 1)
                  (gen/nemesis
                    (fn [] (map gen/once
                                 [{:type :info, :f :stop}
                                  {:type :sleep, :value 60}])))
                  (gen/time-limit 600)))
```

Тест состоит из двух последовательных фаз:

![[Pasted image 20260810175919.png]]

| **Фаза**  | **Операция клиента**              | **Темп**                           | **Длительность**             | **Поведение nemesis**                                                                                    |
| --------- | --------------------------------- | ---------------------------------- | ---------------------------- | -------------------------------------------------------------------------------------------------------- |
| 1. Запись | :add (SQL INSERT)                 | gen/stagger 1/50  ≈50 операций/сек | gen/time-limit 7200 = 2 часа | случайно выбирает 2 отказа подряд из nemesis-starts, держит 60 сек, :stop, пауза 60 сек - и так по кругу |
| 2. Чтение | :read (SQL SELECT ... FOR UPDATE) | gen/stagger 1  1 операция/сек      | gen/time-limit 600= 10 минут | nemesis принудительно :stop; сеть/процессы восстанавливаются.                                            |

```clj
//patroni/jepsen/jepsen/src/jepsen/patroni.clj
(def nemesis-starts [:start-halves :start-ring :start-one :switch :kill])
```

nemesis-starts - это вектор из пяти ключей-меток, на который дважды за цикл ссылается (rand-nth nemesis-starts) в генераторе фазы 1: каждый цикл «60 сек отказа → :stop → 60 сек пауза» случайно выбирает два значения из этого списка и передаёт их как :f в поток nemesis. Вектор служит связующим звеном между генератором (какие ключи вообще могут быть выбраны) и nemesis/compose (описывается в следующем пункте), где для каждого из этих пяти ключей расписано, какой реальный отказ (сетевая разделение, kill, switchover) за ним стоит. В фазе 2 (чтение) этот вектор уже не используется - там :f жёстко задан как :stop, без rand-nth.

### ***Сценарии отказов (nemesis)***

```clj
//patroni/jepsen/jepsen/src/jepsen/patroni.clj
:nemesis   (nemesis/compose {{:start-halves :start} (nemesis/partition-random-halves)
                                {:start-ring   :start} (nemesis/partition-majorities-ring)
                                {:start-one    :start
                                 ; All partitioners heal all nodes on stop so we define stop once
                                 :stop         :stop} (nemesis/partition-random-node)
                                #{:switch} (switcher)
                                #{:kill} (killer)})
```

| **Ключ**      | **nemesis**                              | **Что делает**                                                                                                          |
| ------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| :start-halves | jepsen.nemesis/partition-random-halves   | делит узлы на две случайные половины и разрывает сеть между ними (классический network split)                           |
| :start-ring   | jepsen.nemesis/partition-majorities-ring | выстраивает узлы в кольцо и рвёт связи так, что каждый узел видит только соседей - более «хитрая» частичная сегментация |
| :start-one    | jepsen.nemesis/partition-random-node     | изолирует один случайный узел от всех остальных                                                                         |
| :switch       | кастомный switcher                       | выполняет patronictl switchover --force на случайном patroni*-узле - принудительная смена лидера без сбоя               |
| :kill         | кастомный killer                         | делает pkill -9 по postgres или patroni (выбор случайный) на случайном patroni-узле - имитация падения процесса         |

```clj
//patroni/jepsen/jepsen/src/jepsen/patroni.clj
(defn killer
  "Executes pkill -9 `procname`"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      this)
    (invoke! [this test op]
             (case (:f op)
               :kill (assoc op :value
                            (try
                              (let [procname (rand-nth [:postgres
                                                        :patroni])
                                    node (rand-nth (filter (fn [x] (string/includes? (name x) "patroni"))
                                                           (:nodes test)))]
                                (control/on node
                                  (control/exec :pkill :-9 :-f procname))
                                (assoc op :value [:killed procname :on node]))
                              (catch Throwable t#
                                (let [m# (.getMessage t#)]
                                  (do (warn (str "Unable to run pkill: "
                                                 m#))
                                      m#)))))))
    (teardown! [this test]
      (info (str "Stopping killer")))
    nemesis/Reflection
    (fs [this] #{})))

(defn switcher
  "Executes switchover"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      this)
    (invoke! [this test op]
             (case (:f op)
               :switch (assoc op :value
                          (try
                              (let [node (rand-nth (filter (fn [x] (string/includes? (name x) "patroni"))
                                                           (:nodes test)))]
                                (control/on node
                                  (control/exec :timeout :10 :patronictl :switchover :--force))
                                (assoc op :value [:switchover :on node]))
                            (catch Throwable t#
                              (let [m# (.getMessage t#)]
                                (do (warn (str "Unable to run switch: "
                                               m#))
                                    m#)))))))
    (teardown! [this test]
      (info (str "Stopping switcher")))
    nemesis/Reflection
    (fs [this] #{})))
```

Оба кастомных nemesis выполняются через control/on/control/exec, то есть по SSH-сессии на выбранный узел.

### ***Проверка корректности (checker) в контексте всего test-map***

```clj
//patroni/jepsen/jepsen/src/jepsen/patroni.clj
(defn patroni-test [patroni-nodes etcd-nodes]
  {:nodes     (concat patroni-nodes etcd-nodes)
   :name      "patroni"
   :os        os/noop
   :db        (db)
   :ssh       {:private-key-path "/root/.ssh/id_rsa"}
   :net       net/iptables
   :client    (pg-client nil)
   :nemesis   (nemesis/compose {...})
   :generator (gen/phases ...)
   :checker   patroni-set
   :remote    control/ssh})
```

В test-map единственный checker - patroni-set. Никаких дополнительных встроенных checker'ов (например, checker/perf для графиков latency, как это сделано в postgres Jepsen-тесте через merge с jepsen.tests/noop-test) в patroni.clj не подключено.

### ***DB и OS не выполняют реальной настройки***

```clj
//patroni/jepsen/jepsen/src/jepsen/patroni.clj
(defn db
  "PostgreSQL database"
  []
  (reify db/DB
    (setup! [_ test node]
      (info (str (name node) " setup")))

    (teardown! [_ test node]
      (info (str (name node) " teardown")))))
```

```clj
//patroni/jepsen/jepsen/src/jepsen/patroni.clj
:os os/noop
:db (db)
```

В отличии от теста для postgres в тесте Patroni:

- :os = os/noop - Jepsen не выполняет setup!/teardown! операционной системы вообще.
- :db = кастомная реализация db/DB, у которой setup!/teardown! только логируют, ничего не устанавливают и не перезапускают.

Вся реальная подготовка окружения (установка пакетов, PostgreSQL, Patroni, etcd, SSH-ключи, поднятие кластера) вынесена из цикла Jepsen наружу - в Dockerfile/docker-compose.yml/runit/run.sh. То есть кластер поднимается один раз при старте контейнеров, а db/cycle! с его ретраями и with-os из jepsen.core в этом тесте фактически являются no-op обёртками.

### ***Схема тестового стенда***

![[Pasted image 20260811093839.png]]

Все шесть контейнеров (patroni1..3, etcd1..3) и контейнер test находятся в одной docker-сети jepsen. Клиентская нагрузка и SSH-команды nemesis идут от test к узлам patroni; узлы patroni хранят состояние лидера/конфигурации в etcd штатным для Patroni образом (через DCS-клиент, не через Jepsen).

### ***Способ запуска теста локально***

```clj
//patroni/jepsen/jepsen/test/jepsen/patroni_test.clj
(def patroni_nodes ["patroni1" "patroni2" "patroni3"])
(def etcd_nodes ["etcd1" "etcd2" "etcd3"])

(deftest patroni-test
  (is (:valid? (:results (jepsen/run! (patroni/patroni-test patroni_nodes etcd_nodes))))))
```

```clj
//patroni/jepsen/jepsen/run.sh
#!/bin/bash
set -ex
cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

for n in {1..3}; do
    ssh-keyscan -t rsa "patroni$n" >> /root/.ssh/known_hosts
    ssh-keyscan -t rsa "etcd$n" >> /root/.ssh/known_hosts
done

wait_timeout=60
for (( n=1; n <= wait_timeout; n++ )); do
    leader=$(ssh patroni1 "patronictl list -f json | jq -r '.[] | select(.Role==\"Leader\") | .Member'")
    [ -n "$leader" ] && break
    [ "$n" -eq $wait_timeout ] && exit 1
    sleep 1
done

ssh "$leader" "psql -U postgres -c 'CREATE TABLE IF NOT EXISTS set (value integer primary key)'"
for member in $(ssh "$leader" "patronictl list -f json | jq -r '.[] | select(.Role!=\"Leader\") | .Member'"); do
    for (( n=1; n <= wait_timeout; n++ )); do
        ssh "$member" "psql -U postgres -tAc 'SELECT * FROM set'" && break
        [ "$n" -eq $wait_timeout ] && exit 1
        sleep 1
    done
done

timeout 10800 lein test

```

Последовательность запуска:

1. docker compose up -d --build поднимает 7 контейнеров (test, etcd1..3, patroni1..3) из Dockerfile/docker-compose.yml.
2. В контейнере test запускается run.sh, который: собирает SSH known_hosts для всех 6 узлов → ждёт (до 60 сек) появления лидера Patroni → создаёт таблицу set на лидере → проверяет, что реплики её видят → запускает lein test с общим таймаутом 10800 сек = 3 часа (это внешний лимит; внутренний генератор укладывается в 2 ч + 10 мин = 2 ч 10 мин, оставшееся время - запас на подготовку/сборку истории/checker).
3. lein test вызывает jepsen/run! из patroni_test.clj, который собирает test-map через patroni-test и выполняет полный цикл Jepsen (генератор → nemesis → checker), результат - :valid? в отчёте.

### ***Интеграция в CI***

```yaml
//patroni/.github/workflows/jepsen.yaml
name: Jepsen tests

on:
#  schedule:
#  - cron: '0 0 * * *'

  # Allow manual triggering with a branch input
  workflow_dispatch:
    inputs:
      branch:
        description: 'Branch to run Jepsen tests on'
        required: true
        default: 'master'

jobs:
  jepsen:
    runs-on: ubuntu-latest
    env:
      BRANCH: ${{ github.event.inputs.branch || 'master' }}
    steps:
    - name: Checkout
      uses: actions/checkout@v5

    - name: Validate branch
      run: git ls-remote --exit-code origin "refs/heads/$BRANCH" || (echo "Branch $BRANCH does not exist" && exit 1)

    - name: Run test
      run: |
        cd jepsen
        docker compose build --build-arg "BRANCH=$BRANCH"
        docker compose up -d
        mkdir -p logs
        (
          cd logs
          if ! docker exec jepsen-test bash run.sh > jepsen.log 2>&1; then
            tail -n 18 jepsen.log
            for name in $(docker ps --filter "name=jepsen-[^t]" --format "{{.Names}}"); do
              docker logs "$name" > "${name#jepsen-}.log"
            done
            for name in $(docker ps --filter "name=jepsen-patroni" --format "{{.Names}}"); do
              docker exec "$name" sh -c 'cat $PGDATA/log/postgres.log' > "${name#jepsen-}-postgres.log"
              docker exec "$name" sh -c 'cat $PGDATA/log/postgres.csv' > "${name#jepsen-}-postgres.csv"
            done
            docker exec "$name" patronictl list > patronictl.log
            docker exec "$name" patronictl history >> patronictl.log
            exit 1
          fi
          tail -n 4 jepsen.log
        )
        docker compose down --rmi all

    - name: Upload logs if Jepsen test failed
      uses: actions/upload-artifact@v4
      if: failure()
      with:
        name: jepsen-logs
        path: jepsen/logs
        if-no-files-found: error
        retention-days: 5
```

CI-интеграция есть, но она принципиально отличается от основного тестового workflow:

| **Параметр**            | **.github/workflows/tests.yaml (основные тесты)**                               | **.github/workflows/jepsen.yaml**                                                                                                                                                             |
| ----------------------- | ------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Триггер                 | pull_request, push в master/REL* - автоматически на каждый PR                   | Только workflow_dispatch (запуск вручную из UI Actions, с указанием ветки); плановый schedule присутствует в файле, но закомментирован - то есть сейчас jepsen-тест по расписанию не гоняется |
| Что запускает           | Юнит-тесты и flake8 на матрице ОС/версий Python (install_deps.py, run_tests.py) | Пересборку docker-стенда, run.sh внутри контейнера jepsen-test, сбор логов PostgreSQL/Patroni/patronictl при падении                                                                          |
| Длительность            | Минуты                                                                          | Часы (внутренний таймаут теста - 3 часа)                                                                                                                                                      |
| Необходимость для мержа | Да                                                                              | Нет, отдельный ручной/потенциально плановый прогон                                                                                                                                            |

Технически интеграция в CI возможна и уже сделана, но она осознанно вынесена из обязательного пайплайна из-за длительности (часы против минут) и, вероятно, из-за некоторой недетерминированности такого рода chaos-тестов.
### ***Является ли Jepsen частью основного набора тестов Patroni?***

Jepsen-тест -  не является частью основного набора юнит-/функциональных тестов Patroni:

- находится в изолированном каталоге jepsen/ со своим стеком (Clojure/Leiningen), а не в tests/ (Python/unittest), где распологаются обычные тесты Patroni;
- использует собственную систему сборки и зависимостей (project.clj, lein), никак не связанную с requirements*.txt/tox.ini основного проекта;
- запускается отдельным CI-workflow (jepsen.yaml) вручную/потенциально по расписанию, а не как шаг основного tests.yaml, который обязателен для каждого PR;
- по своей природе - не unit/functional-тест конкретной функции, а долгий chaos-/эксплуатационный тест целого поднятого кластера с реальными сетевыми и процессными сбоями.

### _**14. Вывод: какие ошибки должен обнаруживать этот тест**_

Исходя из checker'а patroni-set и сценария нагрузки/отказов, тест нацелен на обнаружение:

- Потери подтверждённых записей (lost) - ситуаций, когда клиент получил :ok на insert, но после failover/switchover эта запись пропадает из финального чтения. Это прямое нарушение durability и главный класс проблем, которые ищет тест: например, потеря записей при недостаточно строгой синхронной репликации, слишком раннем промоушене реплики без применения всего WAL, либо гонках вокруг synchronous_mode/synchronous_mode_strict.
- Появления «лишних»/сфабрикованных значений (unexpected) - данных в финальном чтении, которых клиент не писал; на практике это скорее сигнал о порче состояния DCS/потере согласованности между узлами, чем типичная находка для PostgreSQL, но checker формально это тоже отслеживает.
- Сценарии, при которых эти ошибки могут проявиться: разрыв сети между лидером и репликами/etcd (:start-halves, :start-ring, :start-one), убийство процесса postgres или patroni на случайном узле (:kill), принудительный switchover в момент активной записи (:switch) - то есть именно тот класс отказов, при которых Patroni обязан либо корректно продолжить работу с сохранением данных, либо явно отказать в записи.



