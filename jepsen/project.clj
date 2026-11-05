(defproject jepsen.patroni "0.1.0-SNAPSHOT"
  :description "Patroni tests"
  :url "https://github.com/patroni/patroni"
  :main jepsen.patroni
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/tools.nrepl "0.2.13"]
                 [clojure-complete "0.2.5"]
                 [jepsen "0.2.6"]
                 [clj-commons/clj-yaml "1.0.30"]
                 [org.clojure/data.json "2.4.0"]
                 [http-kit "2.5.3"]])
