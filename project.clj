(defproject catch-em-all "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :java-source ["src/java/"]
  :license {:name "Apache Public License 2.0"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.fasterxml.jackson.core/jackson-databind "2.3.1"]]
  :profiles {:dev
             {:dependencies [[org.apache.storm/storm-core "0.9.1-incubating"]]}
             :provided
             {:dependencies [[org.apache.storm/storm-core "0.9.1-incubating"]]}})
