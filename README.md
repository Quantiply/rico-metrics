Jobs for Processing Metrics
===

Getting started
---    
1. Setup the env by running
 
        pip install -r requirements.txt
        source bin/set_env.bash
        rico install-deps
        rico build jar
        rico test
        
Building a Package
---
        rico install-deps
        rico package
        
Deploying to Maven repository
---
        rico deploy
        
Fetching from Maven repository
         mvn dependency:get -DremoteRepositories=http://s3.amazonaws.com/artifacts.quantezza.com/release -Dartifact=com.quantiply.rico:rico-metrics:0.0.9:tar.gz:dist
   
Samza Metrics to StatsD
---
0. Test with Command Line Runner

        cat data/samza-metrics.json | rico local samza-to-statsd
 
1. Test with Samza ThreadJobFactory
    
        grid install all #only need to do this once
        rm -rf /tmp/kafka-logs/ && rm -rf /tmp/zookeeper/
        grid start all
        create_topics.sh
        load_topics.sh
        rico samza samza-to-statsd
        #See output
        ./deploy/confluent/bin/kafka-console-consumer --topic sys.statsd --zookeeper localhost:2181 --from-beginning
        grid stop all
    
2. Test with YARN

		grid install all #only need to do this once
      	rm -rf /tmp/kafka-logs/ && rm -rf /tmp/zookeeper/
      	grid start all
      	create_topics.sh
    	load_topics.sh
		grid start yarn
		rico yarn samza-to-statsd
		grid stop yarn
		grid stop all

Druid Metrics to StatsD
---
0. Test with Command Line Runner

        cat data/druid-metrics.json | rico local druid-to-statsd
