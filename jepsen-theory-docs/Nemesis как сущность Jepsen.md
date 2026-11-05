### ***Hазначение Nemesis в Jepsen***

Nemesis - это компонент теста, который намеренно вносит отказы в тестируемую систему (сетевые разделения, убийство процессов, рассинхронизацию часов и т.д.) параллельно с рабочей нагрузкой от Client, чтобы проверить, сохраняет ли система свои гарантии (durability, консистентность) при сбоях. Как и Client, Nemesis реализует протокол (nemesis/Nemesis с методами setup!, invoke!, teardown!) и получает операции от Generator, только выполняет их не над данными, а над инфраструктурой/окружением теста. Результат работы Nemesis (какая атака когда была запущена/остановлена) также попадает в общую history теста - это позволяет checker'у и человеку впоследствии соотносить аномалии в данных с конкретными отказами по времени.
### ***Стандартные Jepsen nemesis: структура по namespace'ам***

Стандартные nemesis разложены по нескольким namespace'ам, которые решают разные задачи - от низкоуровневого воздействия до готовых "пакетов", комбинирующих несколько атак сразу.

#### ***jepsen.nemesis - базовый слой***

##### ***Сетевое разделение***

Все виды разделения реализованы через общую конструкцию partitioner, которая отвечает на :start (наложить grudge - карту "кто с кем не может общаться") и :stop (снять все ограничения через jepsen. net/heal!). Отличаются только функцией вычисления grudge:

```clj
//jepsen/src/jepsen/nemesis
(defn partition-halves []
  (partitioner (comp complete-grudge bisect)))

(defn partition-random-halves []
  (partitioner (comp complete-grudge bisect rand/shuffle)))

(defn partition-random-node []
  (partitioner (comp complete-grudge split-one)))

(defn partition-majorities-ring []
  (partitioner majorities-ring))
```

| **Nemesis**               | **Поведение**                                                                                                               |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| partition-halves          | Режет сеть пополам детерминированно: первые узлы vs остальные                                                               |
| partition-random-halves   | То же самое, но узлы предварительно перемешиваются                                                                          |
| partition-random-node     | Изолирует одну случайную ноду от всех остальных                                                                             |
| partition-majorities-ring | Кольцевая топология: каждый узел видит какое-то большинство кластера, но ни у каких двух узлов это большинство не совпадает |

bridge - не отдельный nemesis, а вспомогательная grudge-функция: режет сеть пополам, но берёт первый узел из второй половины и делает его "мостом" - убирает его из grudge-карты как ключ и из значений всех остальных узлов, так что он сохраняет связь с обеими половинами, а сами половины остаются изолированы друг от друга.

majorities-ring реализована двумя алгоритмами в зависимости от размера кластера:
- majorities-ring-perfect (≤5 узлов) - тасует узлы, зацикливает их в кольцо, берёт скользящие окна размером majority, и для каждого окна вычисляет "центральный" узел и тех, с кем он не связан.
- majorities-ring-stochastic (>5 узлов) - итеративно строит граф связей, на каждом шаге соединяя два наименее связанных узла, пока у каждого не наберётся связей на majority. Не даёт строгую кольцевую структуру, но гарантирует нужное свойство для больших кластеров.

##### ***Воздействие на время***

clock-scrambler - рандомизирует системные часы на всех узлах в пределах окна ±dt секунд (проверка устойчивости к рассинхронизации/скачкам времени). При teardown! возвращает часы к текущему времени.

##### ***Воздействие на процессы***

hammer-time - приостанавливает процесс через SIGSTOP на выбранном узле(ах), возобновляет через SIGCONT при :stop. 

##### ***Воздействие на файловую систему/ данные***

- truncate-file - обрезает последние N байт указанного файла (или случайного файла из директории) на заданных узлах.
- bitflip (Bitflip) - вносит случайные битовые инверсии в файл с заданной вероятностью, используя внешнюю утилиту bitflip, устанавливаемую на узлы при setup!.

##### ***Инфраструктура для комбинирования nemesis***

| **Конструкция**    | **Назначение**                                                                                                                                                                                                                                                                                                                   |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| noop               | Ничего не делает - заглушка                                                                                                                                                                                                                                                                                                      |
| validate(Validate) | Обёртка-валидатор: проверяет, что invoke! обёрнутого nemesis вернул корректный ответ - мапу с :type :info, тем же :process и тем же :f, что и во входной операции; при нарушении бросает исключение.                                                                                                                             |
| timeout            | Обёртка, добавляющая таймаут поверх любого nemesis - если тот не отвечает вовремя, операция считается просроченной (:value :timeout)                                                                                                                                                                                             |
| compose            | Объединяет несколько nemesis в один, маршрутизируя операции по значению :f. Существует в двух режимах: MapCompose - когда передана явная мапа {fs → nemesis}  (маршрутизация по этой мапе), и ReflCompose - когда передана просто коллекция nemesis (маршрутизация вычисляется автоматически через Reflection/fs каждого из них) |


#### ***jepsen.nemesis.file - файловые атаки (расширенная версия)***

Отдельный namespace для повреждения данных на диске, значительно более мощный, чем bitflip/truncate-file из базового jepsen.nemesis. Использует внешние C-программы (corrupt-file), который компилируется также посредством Jepsen функцией nemesis/compile-c-resource!(Что означает, что при написании своих nemesis'ов на си можно переиспользовать готовые функции компиляции кода)

#### ***jepsen.nemesis.time - атаки на часы (расширенная версия)***

Отдельный namespace для более детальной работы с системным временем, чем простой clock-scrambler. Ключевая конструкция - clock-nemesis, принимающая четыре типа операций:
:reset - синхронизация времени с NTP;
:strobe - быстрое "мигание" часов туда-сюда с заданными delta/period/duration;
:bump - одномоментный сдвиг времени на delta мс;
:check-offsets - снять текущее рассогласование без изменений.
Реализация опирается на внешние C-программы (bump-time, strobe-time).

#### ***jepsen.nemesis.combined - комбинирование атак***

Принципиально другой уровень абстракции - не отдельный nemesis, а фреймворк для сборки готового набора nemesis+generator. Ключевая единица - package: мапа вида {:generator ... :final-generator ... :nemesis ... :perf ...}.

Каждый вид отказа реализован как отдельный package-конструктор:

| **Package**             | **За что отвечает**                                                  | **Что умеет**                                                                                                               |
| ----------------------- | -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| db-package              | Убийство/пауза процесса БД                                           | :kill/:start, :pause/:resume через протоколы db/Process/db/Pause - работает, только если сама реализация DB их поддерживает |
| partition-package       | Сетевое разделение                                                   | Использует grudge-настройки: :one, :majority, :majorities-ring, :minority-third, :primaries                                 |
| packet-package          | Порча/задержка/потеря пакетов (деградация связи, а не полный разрыв) | jepsen. net/shape! - delay, loss, corrupt, duplicate и т.п. на выбранных узлах                                              |
| clock-package           | Сдвиг/скачки часов                                                   | Обёртка над jepsen.nemesis.time                                                                                             |
| file-corruption-package | Повреждение файлов                                                   | Обёртка над jepsen.nemesis.file                                                                                             |
|                         |                                                                      |                                                                                                                             |

Общий выбор узлов для всех package'ей идёт через единую функцию jepsen.nemesis.combined/db-nodes:

```clj
//jepsen/src/jepsen/nemesis.combined.clj
(defn db-nodes [test db node-spec]
  (case node-spec
    nil              (rand/nonempty-subset nodes)
    :one             (list (rand/nth nodes))
    :minority        (take (dec (majority (count nodes))) (rand/shuffle nodes))
    :majority        (take      (majority (count nodes))  (rand/shuffle nodes))
    :minority-third  (take (minority-third (count nodes)) (rand/shuffle nodes))
    :primaries       (rand/nonempty-subset (db/primaries db test))
    :all             nodes
    node-spec))
```

### ***Кастомные nemesis в Patroni-тесте***

Из стандартного набора Patroni-тест использует только три функции для разделения сети между узлами  (partition-random-halves, partition-majorities-ring, partition-random-node). Дополнительно тест содержит два кастомных nemesis, написанных вручную под Patroni: killer и switcher.

killer — имитация падения процесса:
```clj
//patroni/jepsen/src/patroni.clj
(defn killer []
  (reify nemesis/Nemesis
    (invoke! [this test op]
      (case (:f op)
        :kill (assoc op :value
                (try
                  (let [procname (rand-nth [:postgres :patroni])
                        node (rand-nth (filter (fn [x] (string/includes? (name x) "patroni"))
                                                (:nodes test)))]
                    (control/on node
                      (control/exec :pkill :-9 :-f procname))
                    (assoc op :value [:killed procname :on node]))
                  (catch Throwable t# ...)))))
    nemesis/Reflection
    (fs [this] #{})))
```

Реальная команда на узле: pkill -9 -f <postgres|patroni> - жёсткое (SIGKILL) завершение процесса. Какой именно процесс убить, выбирается случайно на каждый вызов.
Какие узлы могут быть выбраны: только узлы, чьё имя содержит "patroni" - то есть patroni1..3. Узлы etcd1..3 этой атакой не затрагиваются.

switcher — принудительная смена лидера:

```clj
//patroni/jepsen/src/patroni.clj
(defn switcher []
  (reify nemesis/Nemesis
    (invoke! [this test op]
      (case (:f op)
        :switch (assoc op :value
                  (try
                    (let [node (rand-nth (filter (fn [x] (string/includes? (name x) "patroni"))
                                                  (:nodes test)))]
                      (control/on node
                        (control/exec :timeout :10 :patronictl :switchover :--force))
                      (assoc op :value [:switchover :on node]))
                    (catch Throwable t# ...)))))
    nemesis/Reflection
    (fs [this] #{})))
```

Реальная команда на узле: timeout 10 patronictl switchover --force - принудительная смена лидера кластера с ограничением по времени 10 секунд на выполнение самой команды.
Какие узлы могут быть выбраны: так же, только среди patroni-узлов (patroni1..3), поскольку patronictl - управляющая утилита самого Patroni.
Оба кастомных nemesis выполняются одинаково - через SSH-сессию на выбранный узел: (control/on node (control/exec ...)).


postgres-тест не пишет свой nemesis/compose вручную - он полностью полагается на jepsen.nemesis.combined/nemesis-package:
```clj
//postgres/jepsen/src/postgres.clj
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

```

Patroni-тест, напротив, обходится без combined вовсе - собирает nemesis/compose напрямую из базового jepsen.nemesis, потому что стандартные db-package/kill/pause не покрывают специфичных для Patroni операций - для этого и понадобились кастомные killer/switcher:

```clj
//patroni/jepsen/src/patroni.clj
(defn patroni-test
  [patroni-nodes etcd-nodes]
  {:nodes     (concat patroni-nodes etcd-nodes)
   :name      "patroni"
   :os        os/noop
   :db        (db)
   :ssh       {:private-key-path "/root/.ssh/id_rsa"}
   :net       net/iptables
   :client    (pg-client nil)
   :nemesis   (nemesis/compose {{:start-halves :start} (nemesis/partition-random-halves)
                                {:start-ring   :start} (nemesis/partition-majorities-ring)
                                {:start-one    :start
                                 ; All partitioners heal all nodes on stop so we define stop once
                                 :stop         :stop} (nemesis/partition-random-node)
                                #{:switch} (switcher)
                                #{:kill} (killer)})
```

### ***Восстановление после отказа***

- Сетевые разделение (:start-halves, :start-ring, :start-one) - восстанавливаются автоматически через единый :stop, который в nemesis/compose замаплен сразу на все три nemesis (в коде явный комментарий: "All partitioners heal all nodes on stop so we define stop once"). Механически это снятие сетевых ограничений через jepsen. net/heal! внутри каждого partitioner.

- :kill и :switch — у этих nemesis нет собственного восстановительного действия. pkill и patronictl switchover — разовые команды, не создающие "открытого" состояния, которое нужно было бы явно закрывать. Восстановление полностью полагается на сам Patroni: убитый процесс должен быть перезапущен супервизором (runit), а роль лидера - восстановлена штатным failover-механизмом Patroni через etcd. Jepsen намеренно не помогает системе восстановиться после :kill/:switch, а проверяет, справится ли Patroni сам.

### ***Какие типы проблем моделирует каждый сценарий***

| **Nemesis**                            | **Что происходит физически**                                           | **Какой класс проблем Patroni проверяет**                                                                                                       |
| -------------------------------------- | ---------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| :start-halves(partition-random-halves) | Сеть режется на две случайные половины                                 | Поведение при полном расколе кластера - остаётся ли ровно один лидер (в большинстве), не теряются ли/не раздваиваются ли данные в меньшей части |
| :start-ring(partition-majorities-ring) | Кольцевая частичная сегментация - каждый узел видит разное большинство | Более тонкие сценарии частичной связности, при которых наивный кворумный алгоритм может дать сбой                                               |
| :start-on(partition-random-node)       | Изоляция одного случайного узла                                        | Поведение при потере связи с одним узлом (в т.ч. с текущим лидером) - корректность failover                                                     |
| :kill                                  | pkill -9 по postgres или patroni на случайном узле                     | Восстановление после аварийного падения процесса - скорость перезапуска, сохранность подтверждённых записей                                     |
| :switch                                | Принудительный patronictl switchover --force во время активной записи  | Гонки при плановой смене лидера "на живую" - потеря или дублирование данных в момент передачи роли лидера                                       |

Во всех случаях итоговая проверка идёт через checker patroni-set: 

```clj
//patroni/jepsen/src/patroni.clj
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


после каждого сценария Patroni обязан либо сохранить все подтверждённые (:ok) записи в финальном чтении (иначе - lost, нарушение durability), либо не допустить появления записей, которых клиент не писал (unexpected, признак порчи состояния). Именно эти два инварианта - основной предмет проверки для всех перечисленных сценариев отказов.


*Про взаимодействие nemesis с генератором будет описано в документе про генератор*
