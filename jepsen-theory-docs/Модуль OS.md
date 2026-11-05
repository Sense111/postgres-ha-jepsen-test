### ***Роль компонента OS в Архитектуре Jepsen:***

Модуль :os отвечает за жизненный цикл узлов кластера - их подготовку к тестам и финальную очистку. В него вынесена вся специфика конкретных ОС и дистрибутивов: управление пакетами, настройка сети/hostname, а также установка системных утилит для работы Jepsen и его nemesis-компонентов.

Реализация :os передаётся в тест как обычный параметр наравне с :db, :nemesis, :checker и т. д. и вызывается фреймворком автоматически на каждом узле в начале и в конце прогона теста.

### ***Протокол OS из jepsen.os и назначение его методов***

Протокол определён в модуле jepsen.os:

```clj
//jepsen/src/jepsen/os.clj
(defprotocol OS  
  (setup!    [os test node] "Set up the operating system on this particular node.")  
  (teardown! [os test node] "Tear down the operating system on this particular node."))
```

| **Функция** | **Аргументы** | **Назначение**                                                                                                                                                                       |
| ----------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| setup!      | os,test,node  | Первоначальная подготовка узла перед тестом: настройка hostname/hosts, обновление и установка пакетов, подготовка утилит для Jepsen и nemesis.                                       |
| teardown!   | os,test,node  | Во всех изученных реализациях (CentOS, SmartOS, Debian, Ubuntu) функция объявлена, но фактически пуста -  никакой очистки узла (удаления пакетов, отката конфигурации) не выполняет. |
В модуле также определена вспомогательная реализация-заглушка noop — реализация протокола OS через reify, у которой оба метода ничего не делают:

```clj
//jepsen/src/jepsen/os.clj
(def noop  
  "Does nothing"  
  (reify OS  
    (setup!    [os test node])  
    (teardown! [os test node])))

```
noop используется, когда настройка ОС не требуется (например, при работе с уже подготовленным/существующим окружением.
### ***Существующие реализации OС***

В исходном коде Jepsen найдены следующие реализации протокола OS:

| **ОС**  | **Особенности реализации**                                                                                                                                                                                                                                                          |
| ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| CentOS  | Настройка hostfile, обновление и установка пакетов, установка утилиты управления фоновыми процессами (start-stop-daemon).                                                                                                                                                           |
| SmartOS | Идентична CentOS, за исключением того, что не реализована установка утилиты управления фоновыми процессами.                                                                                                                                                                         |
| Debian  | Расширяет набор функций CentOS новыми: add-key! (добавление GPG ключа стороннего репозитория), add-repo! (добавление стороннего репозитория), install-jdk11!(установка Java  Development Kit). Как и SmartOS, не содержит функции установки утилиты управления фоновыми процессами. |
| Ubuntu  | Полностью заимствует (переиспользует) реализацию Debian; отличается только меньшим количеством пакетов к установке, так как часть пакетов включены в стандартную сборку.                                                                                                            |


- **setup-hostfile!** (CentOS/SmartOS) — приводит /etc/hosts, на удаленных узлах через SSH-сессию control/exec, к корректному виду: для строк, начинающихся с 127.0.0.1, если имя узла ещё не встречается в строке — дописывает его в конец строки (127.0.0.1 → localhost host); В реализации Debian та же по названию функция устроена иначе.
- **time-since-last-update** — возвращает количество секунд, прошедших с последнего обновления пакетов;
- **update!** — обновляет пакеты;
- **maybe-update!** — вызывает update!, если с последнего обновления прошло более 86400 секунд;
- **installed** — получает на вход список пакетов, парсит вывод команды получения установленных пакетов, приводит к set и оставляет только реально установленные;
- **uninstall!** — принимает один пакет или список пакетов, через installed определяет, какие реально установлены, логирует список удаляемых и удаляет их с флагом автоподтверждения (-y);
- **installed?** — проверяет, установлен/ны ли пакет/пакеты;
- **installed-version** — по имени пакета возвращает установленную версию либо nil(пустоту);
- **install** — устанавливает пакеты из списка, если они ещё не установлены; поддерживает установку конкретной версии; пакеты можно передавать одиночным значением, списком или вектором;
- **install-start-stop-daemon!** — устанавливает утилиту управления фоновыми процессами (в CentOS по умолчанию отсутствует, реализована только там);
- **installed-start-stop-daemon?** — проверяет, установлена ли эта утилита.



### ***Использование Debian как OS, для реализации Jepsen-теста Postgres*** 

Подключение namespace OS в cli:

```clj 
//postgres-test/postgres/src/jepsen/postgres/cli.clj
(ns jepsen.postgres.cli
  "Constructs tests, handles CLI arguments, etc."
  (:gen-class)
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java [io :as io]]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [db :as jdb]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [os :as os]
                    [sql :as sql]
                    [tests :as tests]
                    [util :as util]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os.debian :as debian]
            [jepsen.postgres [client :as client]
                             [db :as db]]))
```

Передача OS в карту теста:

По умолчанию debian/os будет устанавливать необходимые пакеты, которых ещё нет в системе.
Но с флагом –existing-postgres будет использоваться существующая конфигурация узла.

```clj 
//postgres-test/postgres/src/jepsen/postgres/cli.clj
(defn postgres-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (let [workload-name (:workload opts)
        workload      ((workloads workload-name) opts)
        db            (if (:existing-postgres opts)
                        jdb/noop
                        (db/db opts))
        os            (if (:existing-postgres opts)
                        os/noop
                        debian/os)
        nemesis       (if (:existing-postgres opts)
                        {:nemesis nemesis/noop}
                        (nc/nemesis-package
                          {:db        db
                           :nodes     (:nodes opts)
                           :faults    (:nemesis opts)
                           :partition {:targets [:primaries]}
                           :pause     {:targets [nil :one :primaries :majority :all]}
                           :kill      {:targets [nil :one :primaries :majority :all]}
                           :interval  (:nemesis-interval opts)}))]
    (merge tests/noop-test
           opts
           {:name (str "postgres " (name workload-name)
                       " " (short-isolation (:isolation opts)) " ("
                       (short-isolation (:expected-consistency-model opts)) ")"
                       (when-let [ns (:nemesis opts)]
                         (str " " (str/join "," (map name (:nemesis opts))))))
            :os   os
            :db   db
            :checker (checker/compose
                       {:perf       (checker/perf
                                      {:nemeses (:perf nemesis)})
                        :clock      (checker/clock-plot)
                        :stats      (checker/stats)
                        :exceptions (checker/unhandled-exceptions)
                        :workload   (:checker workload)})
            :client    (:client workload)
            :nemesis   (:nemesis nemesis)
            :generator (gen/phases
                         (->> (:generator workload)
                              (gen/stagger (/ (:rate opts)))
                              (gen/nemesis (:generator nemesis))
                              (gen/time-limit (:time-limit opts))))}
           ; If we're using an existing postgres install, disable all SSH
           ; capabilities, including fault injection.
           (when (:existing-postgres opts)
             {:ssh {:dummy? true}}))))

```

Подключение namespace OS в db: 

```clj
//postgres-test/postgres/src/jepsen/postgres/db.clj

(ns jepsen.postgres.db
  "Database setup and automation."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c :refer [|]]
                    [core :as jepsen]
                    [db :as db]
                    [random :as rand]
                    [util :as util :refer [meh random-nonempty-subset]]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.debian :as debian]
            [jepsen.postgres [client :as sc]]
            [slingshot.slingshot :refer [try+ throw+]]))

```

Использование debian/OS в db:

Автоматизированная установка пакета postgresql.

```clj
//postgres-test/postgres/src/jepsen/postgres/db.clj
(defn install-pg!
  "Installs postgresql"
  [test node]
  (c/su
    (when-not (debian/installed? :postgresql-18)
      ; Add repo
      (debian/install ["postgresql-common"])
      (info "Adding Postgres apt repos")
      (c/exec :echo "" | "/usr/share/postgresql-common/pgdg/apt.postgresql.org.sh")
      ; Install
      (debian/install [:postgresql-18 :postgresql-client-18])
      ; Deactivate default install
      (c/exec :service :postgresql :stop)
      (c/exec "update-rc.d" :postgresql :disable))))

```

### ***Процесс подключения реализации ОС к Jepsen-тесту***

Параметр :os передаётся в опциях теста и участвует в общей карте теста (test map) так же, как :db, :nemesis, :checker. В файле jepsen-postgres, в функции postgres-test.
Само использование :os внутри ядра Jepsen происходит через макрос with-os (jepsen.core):

```clj
//jepsen/src/jepsen/core.clj
(defmacro with-os
  "Wraps body for OS setup and teardown."
  [test & body]
  `(try
     (control/on-nodes ~test (partial os/setup! (:os ~test)))
     ~@body
     (finally
       (control/on-nodes ~test (partial os/teardown! (:os ~test))))))

```

with-os вызывается в основном раннере теста (jepsen.core/ run!) 
вокруг настройки :db и выполнения самого теста, гарантируя, что setup! отработает на всех узлах до старта, а teardown! - после.

```clj
//jepsen/src/jepsen/core.clj
(defn run! “…” 

[test]

  (with-thread-name "jepsen test runner"

    (let [test (prepare-test test)]

      (with-logging test

        (store/with-handle [test test]

          (let [test (if (:name test)

                       (store/save-0! test)

                       test)

                test (with-sessions [test test]

                       ; Launch OS, DBs, evaluate test

                       (let [test (**with-os** test

                                    (with-db test

                                      (util/with-relative-time

                                        ; Run a single case

                                        (let [test (-> (run-case! test)

                                                       ; Remove state

                                                       (dissoc :barrier

                                                               :sessions))

                                              _ (info "Run complete, writing")

                                              test (if (:name test)

                                                     (store/save-1! test)

                                                     test)]

                                          test))))]

                         (analyze! test)))]

            (log-results test)))))))
```

Рассылка на узлы выполняется через control/on-nodes, которая, в свою очередь, использует control/with-nodes*:
	Если узлов меньше 1 (не указаны/пусто) - выполнение идёт в текущем потоке;
	Если узлов больше - выполняется распараллеливание.
	Результат - вывод со всех серверов собирается и упаковывается в отсортированную map (node → результат).
	
```clj
//jepsen/src/jepsen/control.clj
(defn with-nodes*

  "Given a test, evaluates (f node) in parallel on each node, with that

  node's SSH connection bound. If `nodes` is provided, evaluates only on those

  nodes in particular."

  ([test f]

   (with-nodes* test (:nodes test) f))

  ([test nodes f]

   (let [mapper (if (< 1 (count nodes))

                  real-pmap

                  ; A common case: we call with just one node; no need to spawn

                  ; a thread.

                  mapv)]

     (->> nodes

          (map (fn [node]

                 (let [session (get (:sessions test) node)]

                   (assert session (str "No session for node " (pr-str node)))

                   [node session])))

          (mapper (bound-fn [[node session]]

                    (with-thread-name (str "jepsen node " (name node))

                      (with-session node session

                        [node (f node)]))))

          (into (sorted-map))))))

(defn on-nodes

  "Given a test, evaluates (f test node) in parallel on each node, with that

  node's SSH connection bound. If `nodes` is provided, evaluates only on those

  nodes in particular. TODO: deprecate in favor of `with-nodes*``."

  ([test f]

   (on-nodes test (:nodes test) f))

  ([test nodes f]

   (with-nodes* test nodes (partial f test))))
```

### ***ОС-зависимые операции***

| **Категория**               | **Операции/Функции**                                                            | **Зависимость**                                                                                                                                                                                 |
| --------------------------- | ------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Пакетный менеджер           | update!, install, uninstall!, installed?, installed-version                     | Полностью завязано на конкретный пакетный менеджер: yum/rpm (CentOS), pkgin (SmartOS), apt/dpkg --get-selections (Debian/Ubuntu).                                                               |
| Список пакетов              | Конкретный набор устанавливаемых пакетов, необходимых Jepsen/nemesis            | Зависит от дистрибутива: разные имена пакетов для одной и той же функциональности (например, iptables в Debian/CentOS vs ipfilter в SmartOS).                                                   |
| Сетевые nemesis-инструменты | Настройка firewall-реализации через параметр :net, вызов net/heal! после setup! | SmartOS требует явного указания :net jepsen.net/ipfilter, иначе по умолчанию используется реализация на iptables. В Debian вызов обёрнут в meh, ошибки восстановления сети не прерывают setup!. |
| Системные службы/демоны     | install-start-stop-daemon!, installed-start-stop-daemon?                        | В Debian/Ubuntu утилита есть «из коробки», в CentOS требует отдельной установки.                                                                                                                |
| Специфика ОС                | add-key!, add-repo!                                                             | Полностью специфичны для APT-based систем.                                                                                                                                                      |

### ***Модуль DB***

Помимо :os, в тест точно так же подключается отдельный компонент :db (протокол jepsen.db/ DB), отвечающий за саму тестируемую СУБД (в примере - Postgres). 

Протокол DB шире протокола OS. OS ограничивается только setup!/teardown!. DB, помимо них, поддерживает несколько опциональных протоколов, специфичных именно для тестируемой СУБД:

```clj
//jepsen/src/jepsen/db.clj
(defprotocol DB
  (setup!     [db test node] "Set up the database on this particular node.")
  (teardown!  [db test node] "Tear down the database on this particular node."))

(defprotocol Kill
  "This optional protocol supports starting and killing a DB's processes."
  (kill!  [db test node] "Forcibly kills the process")
  (start! [db test node] "Starts the process"))

(defprotocol Pause
  "This optional protocol supports pausing and resuming a DB's processes."
  (pause!   [db test node] "Pauses the process")
  (resume!  [db test node] "Resumes the process"))

(defprotocol Primary
  "This optional protocol supports databases which have a notion of one (or
  more) primary nodes."
  (primaries [db test] "...")
  (setup-primary! [db test node] "Performs one-time setup on a single node."))

(defprotocol LogFiles
  (log-files [db test node]
             "Returns either a.) a map of fully-qualified remote paths ...
             or b.) a sequence of fully-qualified remote paths."))
```

Kill/Pause/Primary/LogFiles нужны немезису и чекеру — убить процесс БД, поставить его на паузу, узнать, кто сейчас primary, собрать логи для отчёта.

В том же неймспейсе jepsen.db есть готовая реализация  tcpdump -  DB-компонент, который  пишет сетевой трафик узла в файл на протяжении всего теста:

```clj
//jepsen/src/jepsen/db.clj
(defn tcpdump
  "A database which runs a tcpdump capture from setup! to teardown!, and yields
  a `tcpdump` logfile. Options:
    :clients-only?  Filter to only Jepsen-client traffic.
    :filter         Extra filter string (e.g. \"host 192.168.122.1\").
    :ports          Collection of ports to grab traffic from."
  [opts]
  (let [dir "/tmp/jepsen/tcpdump", log-file (str dir "/log")
        cap-file (str dir "/tcpdump"), pid-file (str dir "/pid")]
    (reify
      DB
      (setup! [this test node]
        (control/su
          (control/exec :mkdir :-p dir)
          (let [filter-str (->> [(->> (:ports opts) (map (partial str "port ")) (str/join " or "))
                                  (when (:clients-only? opts) (str "host " (cn/control-ip)))
                                  (:filter opts)]
                                 (remove nil?) (str/join " and "))]
            (cu/start-daemon! {:logfile log-file :pidfile pid-file :chdir dir}
                               "/usr/bin/tcpdump"
                               :-w cap-file :-s 65535 :-B 16384 :-U filter-str))))

      (teardown! [this test node]
        (control/su
          (when-let [pid (try+ (control/exec :cat pid-file)
                                (catch [:type :jepsen.control/nonzero-exit] e nil))]
            (meh (control/exec :kill :-s :INT pid)) ; try a clean exit first
            (while (try+ (control/exec :ps :-p pid) true
                         (catch [:type :jepsen.control/nonzero-exit] e false))
              (info "Waiting for tcpdump" pid "to exit")
              (Thread/sleep 50)))
          (cu/stop-daemon! :tcpdump pid-file)
          (control/exec :rm :-rf dir)))

      LogFiles
      (log-files [db test node]
        {log-file "tcpdump.log", cap-file "tcpdump.pcap"}))))
```

Это показывает, что протокол DB в Jepsen шире, чем «установить и снести тестируемую СУБД»: setup!/teardown! - это в целом «запусти вспомогательный процесс на узле в начале теста, останови и собери артефакты в конце», и это может быть как СУБД, так и сторонний инструмент диагностики вроде tcpdump. 

Протокол LogFiles допускает два разных формата возврата: либо готовую карту {удалённый-путь короткое-локальное-имя}, либо просто список удалённых путей (без явных коротких имён) - это видно и по tcpdump (возвращает карту напрямую), и по jepsen.postgres.db (тоже возвращает карту). Но исторически не все реализации DB возвращали карту - часть отдавала просто список строк, поэтому в модуле есть отдельная функция-обёртка:

```clj
//jepsen/src/jepsen/db.clj
(defn log-files-map
  "Takes a DB, a test, and a node. Returns a map of remote paths to local
  paths. Checks to make sure there are no duplicate local paths.

  log-files used to return a sequence of remote paths, and some people are
  likely still assuming that form for composition. When they start e.g.
  concatenating maps into lists of strings, we're going to get mixed
  representations. We try to make all this Just Work (TM)."
  [db test node]
  (let [log-files (log-files db test node)
        log-files (if (map? log-files)
                    log-files
                    (let [short-files (into {} (filter map-entry? log-files))
                          long-files  (remove map-entry? log-files)
                          auto-shorts (->> long-files
                                           (map #(str/split % #"/"))
                                           util/drop-common-proper-prefix
                                           (map (partial str/join "/"))
                                           (zipmap log-files))]
                      (merge short-files auto-shorts)))]
    (assert+ (distinct? (vals log-files))
             {:type       ::log-files-local-name-collision
              :log-files  log-files})
    log-files))
```

Если log-files возвращает карту, она используется как есть. Если возвращается список путей, функция автоматически сокращает их через util/drop-common-proper-prefix, убирая общий префикс каталогов. В конце проверяется отсутствие дубликатов имён, чтобы файлы не перезаписали друг друга при сохранении в store/.

Оркестрация в ядре тоже разная:

```clj
//jepsen/src/jepsen/core.clj
(defmacro with-os
  [test & body]
  `(try
     (control/on-nodes ~test (partial os/setup! (:os ~test)))
     ~@body
     (finally
       (control/on-nodes ~test (partial os/teardown! (:os ~test))))))

(defmacro with-db
  "Wraps body in DB setup and teardown."
  [test & body]
  `(try
     (with-log-snarfing ~test
       (db/cycle! ~test)
       ~@body)
     (finally
       (when-not (:leave-db-running? ~test)
         (control/on-nodes ~test (partial db/teardown! (:db ~test)))))))
```

- with-db оборачивает макрос сборки логов (with-log-snarfing).
- teardown! у DB можно отключить опцией теста :leave-db-running? (удобно, чтобы после прогона руками зайти и посмотреть состояние БД) - у OS teardown! выполняется всегда безусловно.
- DB использует db/cycle! — отдельный механизм с ретраями (до 3 попыток, cycle-tries), который сначала тушит СУБД на всех узлах, затем поднимает её и, если реализован протокол Primary, отдельно настраивает primary-узел. Если setup! бросает исключение с :type ::setup-failed — весь цикл (teardown+setup) повторяется. У OS ничего подобного нет: если setup! падает, тест просто падает целиком.

```clj
//jepsen/src/jepsen/db.clj
(def cycle-tries
  "How many tries do we get to set up a database?"
  3)

(defn cycle!
  "Takes a test, and tears down, then sets up, the database on all nodes
  concurrently.

  If any call to setup! or setup-primary! throws :type ::setup-failed, we tear
  down and retry the whole process up to `cycle-tries` times."
  [test]
  (let [db (:db test)]
    (loop [tries cycle-tries]
      ; Tear down every node
      (info "Tearing down DB")
      (control/on-nodes test (partial teardown! db))

      ; Start up every node
      (if (= :retry (try+
                      (info "Setting up DB")
                      (control/on-nodes test (partial setup! db))

                      ; Set up primary
                      (when (satisfies? Primary db)
                        (info "Setting up primary" (first (:nodes test)))
                        (control/on-nodes test [(first (:nodes test))]
                                          (partial setup-primary! db)))
                      nil
                      (catch [:type ::setup-failed] e
                        (if (< 1 tries)
                          (do (warn (:throwable &throw-context)
                                    "Unable to set up database; retrying...")
                              :retry)
                          (throw+ e)))))
        (recur (dec tries))))))
```

Ключевые моменты cycle!:

- Каждая попытка начинается с безусловного teardown! на всех узлах - то есть перед setup! всегда гарантируется «чистое состояние», даже на первой попытке.
- setup! выполняется параллельно на всех узлах через control/on-nodes; если реализован протокол Primary, отдельным шагом (уже не параллельно, а только на первом узле из :nodes) вызывается setup-primary! — то есть сначала поднимают DB везде, а затем на одном выбранном узле донастраивают его как primary.
- DB сама должна явно бросить ошибку, если считает, что setup не удался и стоит попробовать снова) — до 3 попыток суммарно (cycle-tries), после чего исключение прокидывается дальше и тест падает.
- У OS ничего подобного нет: если os/setup! падает, with-os не делает никаких повторных попыток — тест сразу прерывается.


На практике граница между OS и DB не всегда полностью соблюдается. Наглядный пример — jepsen.postgres.db:

```clj
//postgres-test/postgres/src/jepsen/postgres/db.clj
(ns jepsen.postgres.db
  (:require ...
            [jepsen.os.debian :as debian]
            ...))

(defn install-pg!
  "Installs postgresql"
  [test node]
  (c/su
    (when-not (debian/installed? :postgresql-18)
      (debian/install ["postgresql-common"])
      (info "Adding Postgres apt repos")
      (c/exec :echo "" | "/usr/share/postgresql-common/pgdg/apt.postgresql.org.sh")
      (debian/install [:postgresql-18 :postgresql-client-18])
      (c/exec :service :postgresql :stop)
      (c/exec "update-rc.d" :postgresql :disable))))
    
```

То есть установка пакета самого Postgres (installed?, install, добавление apt-репозитория PGDG) реализована внутри DB, через прямой вызов функций конкретного namespace jepsen.os.debian, зашитый в :require. Получается, что:

- jepsen.postgres.db жёстко завязан на Debian на уровне кода, независимо от того, какая реализация фактически передана в тест через :os. Если в тесте подставить другую реализацию :os (например, для новой debian-based ОС), db.clj всё равно продолжит дёргать debian/install и debian/installed? напрямую - переключение :os никак на это не повлияет.
- Общая с OS инфраструктура пакетного менеджера (install/installed?/uninstall! и пр.) переиспользуется DB-модулем не как абстракция за протоколом, а как обычная библиотека функций конкретного дистрибутива.


### ***Способ добавления Debian-based ОС***

Поскольку Ubuntu уже показывает рабочий паттерн («наследование» от Debian с заменой только списка пакетов»), для добавления новой debian-based ОС предлагается аналогичный подход:

| **Шаг** | **Действие**                                                                                                                                                                                                |
| ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1       | Определить целевой список пакетов для новой ОС. Проверить наличие пакетов в ОС, которые указаны в  Debian-реализации.                                                                                       |
| 2       | Создать неймспейс jepsen.os.<OSname> с функцией os, реализующей протокол OS через reify, аналогично debian/os и ubuntu/os.                                                                                  |
| 3       | В setup! переиспользовать debian/setup-hostfile!, debian/maybe-update!, debian/install , передав новый список пакетов или оставив список, как есть.                                                         |
| 4       | Проверить необходимость add-key!/add-repo! для подключения дополнительных репозиториев, если требуемые пакеты (например, определённая версия PostgreSQL) отсутствуют в стандартных репозиториях целевой ОС. |
| 5       | При необходимости добавить свои специфичные шаги настройки (например, установка jdk, специфичные параметры, дополнительные сервисные скрипты).                                                              |
| 6       | Реализовать/дополнить teardown!, если требуется полноценная очистка узла после теста.                                                                                                                       |
| 7       | Подключить реализацию через :os в конкретном тесте (по аналогии с debian/os в jepsen-postgres/postgres-test).                                                                                               |
### ***Сложность реализации***

- Низкая - если целевая ОС debian-based и близка к Ubuntu/Debian (одинаковый пакетный менеджер apt, схожий набор пакетов в стандартных репозиториях): фактически потребуется только (возможное) измение списка пакетов и обёртка reify, как это сделано для Ubuntu.
- Средняя - если часть требуемых пакетов отсутствует в стандартных репозиториях целевой ОС (потребуется add-repo!/add-key! или сборка из исходников), либо если версии пакетов заметно расходятся (несовместимые опции команд, другие пути к бинарям).
- Высокая - при заметных отличиях от Debian на уровне системных служб/утилит (например, отсутствие/иная реализация start-stop-daemon, iptables, другой init-система).