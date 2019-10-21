<pre>
pingc@vm ~/storm_code/storm_sentence_count master> mvn clean compile package
[INFO] Scanning for projects...
[INFO]                                                                         
[INFO] ------------------------------------------------------------------------
[INFO] Building count_sentence 1.0
[INFO] ------------------------------------------------------------------------
[INFO] 
[INFO] --- maven-clean-plugin:2.5:clean (default-clean) @ count_sentence ---
[INFO] 
[INFO] --- maven-resources-plugin:2.6:resources (default-resources) @ count_sentence ---
[INFO] Using 'UTF-8' encoding to copy filtered resources.
[INFO] Copying 1 resource
[INFO] Copying 2 resources
[INFO] 
[INFO] --- maven-compiler-plugin:3.3:compile (default-compile) @ count_sentence ---
[INFO] No sources to compile
[INFO] 
[INFO] --- maven-resources-plugin:2.6:resources (default-resources) @ count_sentence ---
[INFO] Using 'UTF-8' encoding to copy filtered resources.
[INFO] Copying 1 resource
[INFO] Copying 2 resources
[INFO] 
[INFO] --- maven-compiler-plugin:3.3:compile (default-compile) @ count_sentence ---
[INFO] No sources to compile
[INFO] 
[INFO] --- maven-resources-plugin:2.6:testResources (default-testResources) @ count_sentence ---
[INFO] Using 'UTF-8' encoding to copy filtered resources.
[INFO] skip non existing resourceDirectory /home/pingc/storm_code/storm_sentence_count/src/test/resources
[INFO] 
[INFO] --- maven-compiler-plugin:3.3:testCompile (default-testCompile) @ count_sentence ---
[INFO] No sources to compile
[INFO] 
[INFO] --- maven-surefire-plugin:2.17:test (default-test) @ count_sentence ---
[INFO] No tests to run.
[INFO] 
[INFO] --- maven-jar-plugin:2.4:jar (default-jar) @ count_sentence ---
[INFO] Building jar: /home/pingc/storm_code/storm_sentence_count/target/count_sentence-1.0.jar
[INFO] 
[INFO] --- maven-shade-plugin:2.3:shade (default) @ count_sentence ---
[INFO] Including commons-cli:commons-cli:jar:1.4 in the shaded jar.
[INFO] Including org.apache.storm:flux-core:jar:2.0.0 in the shaded jar.
[INFO] Including org.apache.storm:flux-wrappers:jar:2.0.0 in the shaded jar.
[INFO] Including org.apache.storm:multilang-javascript:jar:2.0.0 in the shaded jar.
[INFO] Including org.apache.storm:multilang-ruby:jar:2.0.0 in the shaded jar.
[INFO] Including org.apache.storm:multilang-python:jar:2.0.0 in the shaded jar.
[WARNING] flux-core-2.0.0.jar, commons-cli-1.4.jar define 22 overlappping classes: 
[WARNING]   - org.apache.commons.cli.HelpFormatter$OptionComparator
[WARNING]   - org.apache.commons.cli.ParseException
[WARNING]   - org.apache.commons.cli.OptionValidator
[WARNING]   - org.apache.commons.cli.UnrecognizedOptionException
[WARNING]   - org.apache.commons.cli.Parser
[WARNING]   - org.apache.commons.cli.CommandLineParser
[WARNING]   - org.apache.commons.cli.PatternOptionBuilder
[WARNING]   - org.apache.commons.cli.GnuParser
[WARNING]   - org.apache.commons.cli.Option
[WARNING]   - org.apache.commons.cli.TypeHandler
[WARNING]   - 12 more...
[WARNING] flux-wrappers-2.0.0.jar, flux-core-2.0.0.jar define 3 overlappping classes: 
[WARNING]   - org.apache.storm.flux.wrappers.spouts.FluxShellSpout
[WARNING]   - org.apache.storm.flux.wrappers.bolts.LogInfoBolt
[WARNING]   - org.apache.storm.flux.wrappers.bolts.FluxShellBolt
[WARNING] maven-shade-plugin has detected that some .class files
[WARNING] are present in two or more JARs. When this happens, only
[WARNING] one single version of the class is copied in the uberjar.
[WARNING] Usually this is not harmful and you can skeep these
[WARNING] warnings, otherwise try to manually exclude artifacts
[WARNING] based on mvn dependency:tree -Ddetail=true and the above
[WARNING] output
[WARNING] See http://docs.codehaus.org/display/MAVENUSER/Shade+Plugin
[INFO] Replacing original artifact with shaded artifact.
[INFO] Replacing /home/pingc/storm_code/storm_sentence_count/target/count_sentence-1.0.jar with /home/pingc/storm_code/storm_sentence_count/target/count_sentence-1.0-shaded.jar
[INFO] Dependency-reduced POM written at: /home/pingc/storm_code/storm_sentence_count/dependency-reduced-pom.xml
[INFO] Dependency-reduced POM written at: /home/pingc/storm_code/storm_sentence_count/dependency-reduced-pom.xml
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 2.112 s
[INFO] Finished at: 2019-10-21T16:50:40+08:00
[INFO] Final Memory: 23M/264M
[INFO] ------------------------------------------------------------------------
pingc@vm ~/storm_code/storm_sentence_count master> 
pingc@vm ~/storm_code/storm_sentence_count master> storm jar target/count_sentence-1.0.jar org.apache.storm.flux.Flux resources/topology.yaml -c nimbus.seeds='["10.78.68.55","10.78.68.56","10.78.68.57"]'
Running: /usr/lib/jvm/java-8-oracle/bin/java -client -Ddaemon.name= -Dstorm.options=nimbus.seeds%3D%5B%2210.78.68.55%22%2C%2210.78.68.56%22%2C%2210.78.68.57%22%5D -Dstorm.home=/home/pingc/bin/apache-storm-2.0.0 -Dstorm.log.dir=/home/pingc/bin/apache-storm-2.0.0/logs -Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib:/usr/lib64 -Dstorm.conf.file= -cp /home/pingc/bin/apache-storm-2.0.0/*:/home/pingc/bin/apache-storm-2.0.0/lib/*:/home/pingc/bin/apache-storm-2.0.0/extlib/*:target/count_sentence-1.0.jar:/home/pingc/bin/apache-storm-2.0.0/conf:/home/pingc/bin/apache-storm-2.0.0/bin: -Dstorm.jar=target/count_sentence-1.0.jar -Dstorm.dependency.jars= -Dstorm.dependency.artifacts={} org.apache.storm.flux.Flux resources/topology.yaml
███████╗██╗     ██╗   ██╗██╗  ██╗
██╔════╝██║     ██║   ██║╚██╗██╔╝
█████╗  ██║     ██║   ██║ ╚███╔╝
██╔══╝  ██║     ██║   ██║ ██╔██╗
██║     ███████╗╚██████╔╝██╔╝ ██╗
╚═╝     ╚══════╝ ╚═════╝ ╚═╝  ╚═╝
+-         Apache Storm        -+
+-  data FLow User eXperience  -+
Version: 2.0.0
Parsing file: /home/pingc/storm_code/storm_sentence_count/resources/topology.yaml
16:50:50.176 [main] INFO  o.a.s.f.p.FluxParser - loading YAML from input stream...
16:50:50.179 [main] INFO  o.a.s.f.p.FluxParser - Not performing property substitution.
16:50:50.179 [main] INFO  o.a.s.f.p.FluxParser - Not performing environment variable substitution.
16:50:50.217 [main] INFO  o.a.s.f.FluxBuilder - Detected DSL topology...
---------- TOPOLOGY DETAILS ----------
Topology Name: count_sentence
--------------- SPOUTS ---------------
sentence-spout [1] (org.apache.storm.flux.wrappers.spouts.FluxShellSpout)
---------------- BOLTS ---------------
splitter-bolt [1] (org.apache.storm.flux.wrappers.bolts.FluxShellBolt)
--------------- STREAMS ---------------
sentence-spout --SHUFFLE--> splitter-bolt
--------------------------------------
16:50:50.331 [main] INFO  o.a.s.f.Flux - Deploying topology in an ACTIVE state...
16:50:50.383 [main] INFO  o.a.s.StormSubmitter - Generated ZooKeeper secret payload for MD5-digest: -8617678670583788762:-6733750606164230264
^[[A16:50:50.468 [main] WARN  o.a.s.v.ConfigValidation - task.heartbeat.frequency.secs is a deprecated config please see class org.apache.storm.Config.TASK_HEARTBEAT_FREQUENCY_SECS for more information.
16:50:51.054 [main] INFO  o.a.s.u.NimbusClient - Found leader nimbus : 10.78.68.56:6627
16:50:51.278 [main] INFO  o.a.s.s.a.ClientAuthUtils - Got AutoCreds []
16:50:52.198 [main] INFO  o.a.s.StormSubmitter - Uploading dependencies - jars...
16:50:52.199 [main] INFO  o.a.s.StormSubmitter - Uploading dependencies - artifacts...
16:50:52.199 [main] INFO  o.a.s.StormSubmitter - Dependency Blob keys - jars : [] / artifacts : []
16:50:52.423 [main] INFO  o.a.s.StormSubmitter - Uploading topology jar target/count_sentence-1.0.jar to assigned location: /home/nimbus-admin/data/storm/nimbus/inbox/stormjar-8a0f1781-7809-45f8-988b-a392b9a5023e.jar
16:50:57.085 [main] INFO  o.a.s.StormSubmitter - Successfully uploaded topology jar to assigned location: /home/nimbus-admin/data/storm/nimbus/inbox/stormjar-8a0f1781-7809-45f8-988b-a392b9a5023e.jar
16:50:57.085 [main] INFO  o.a.s.StormSubmitter - Submitting topology count_sentence in distributed mode with conf {"nimbus.seeds":["10.78.68.55","10.78.68.56","10.78.68.57"],"storm.zookeeper.topology.auth.scheme":"digest","storm.zookeeper.topology.auth.payload":"-8617678670583788762:-6733750606164230264","topology.workers":1}
16:50:57.605 [main] INFO  o.a.s.StormSubmitter - Finished submitting topology: count_sentence
pingc@vm ~/storm_code/storm_sentence_count master> 
</pre>