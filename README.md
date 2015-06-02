Samza jobs in Jython for processing metrics
===

Getting started
---    
1. Setup the env by running
 
        pip install -r requirements.txt
        source bin/set_env.bash
        bin/rico build jar
        

2. Run the `test` task by 
        
        data/test-json.sh 10 | bin/rico local test

    This processor implements the samza Task API and is run locally by sending data over STDIN and the output is printed to stdout.
    
   
Tutorial - Create a Echo processor
---
0. Make sure you have run the getting started section.
 
1. Create a python module.
    
        cd app
        mkdir tut
        touch tut/__init__.py
        touch tut/echo.py
    
2. Implement the Samza Task [Samza API Overview](http://samza.apache.org/learn/documentation/0.9/api/overview.html)

    ```{python}
    from qply.base import BaseProcessor
    from org.apache.samza.system import OutgoingMessageEnvelope
    
    class EchoProcessor(BaseProcessor):
        def process(self, data, collector, coord):
            collector.send(OutgoingMessageEnvelope(None, data.message))
        
    ```

3. Add a job to `config/jobs.yml`
    
    ```{yaml}
    echo:
      samza:
        job.name: test
        task:
          entrypoint: tut.echo.EchoProcessor
          class: com.quantiply.samza.JythonTask
          inputs: kafka.echo.input
          output: kafka.echo.output
    ```


4. Run this locally :
    
        data/test-json.sh 10 | bin/rico local echo

    You should see the following output.
    
        <snip>
        {"headers":{"hi":"there"},"payload":{"mumbo":"jumbo","id":1}}
        {"headers":{"hi":"there"},"payload":{"mumbo":"jumbo","id":2}}
        {"headers":{"hi":"there"},"payload":{"mumbo":"jumbo","id":3}}
        {"headers":{"hi":"there"},"payload":{"mumbo":"jumbo","id":4}}
        {"headers":{"hi":"there"},"payload":{"mumbo":"jumbo","id":5}}
        {"headers":{"hi":"there"},"payload":{"mumbo":"jumbo","id":6}}
        {"headers":{"hi":"there"},"payload":{"mumbo":"jumbo","id":7}}
        {"headers":{"hi":"there"},"payload":{"mumbo":"jumbo","id":8}}
        {"headers":{"hi":"there"},"payload":{"mumbo":"jumbo","id":9}}
        {"headers":{"hi":"there"},"payload":{"mumbo":"jumbo","id":10}}

5. Now, lets modify the processor so that it can run on Samza.

    ```{python}
    from qply.base import BaseProcessor
    from org.apache.samza.system import OutgoingMessageEnvelope, SystemStream

    class EchoProcessor(BaseProcessor):
        def init(self, config, context):
            self.output = SystemStream("kafka", config.get("task.output").replace("kafka.", ""))
            # Print the config too.
            print("[init] config : " + str(config))
            
        def process(self, data, collector, coord):
            collector.send(OutgoingMessageEnvelope(self.output, data.message))
        
    ```
    

6. Run it on Samza

        bin/grid install all
        bin/grid start zookeeper
        bin/grid start kafka
        
    Create kafka topics.
        
        ./deploy/confluent/bin/kafka-topics --zookeeper localhost:2181  --partitions 1 --replication-factor 1 --create --topic echo.input
        ./deploy/confluent/bin/kafka-topics --zookeeper localhost:2181  --partitions 1 --replication-factor 1 --create --topic echo.output
    
    Run the Samza job in another terminal window.
        
        bin/rico samza echo
        
    You should see a log like this:
    
        <samza log ... >
        
    Push some data into Kafka
    
        data/test-json.sh 10000 | ./deploy/confluent/bin/kafka-console-producer --broker-list localhost:9092 --topic echo.input
        
    See if the output topic has data now 

        ./deploy/confluent/bin/kafka-console-consumer --zookeeper localhost:2181 --topic echo.output --from-beginning
        
7. Let's run it on YARN.
    
        bin/grid run yarn
        bin/rico yarn echo
        
    Now send some more data to Kafka input topic and watch the output topic as we did above.
    
        The YARN console is at : localhost:8088
        
        
        
Code Loading
---

1. The rico script launches the Jython Samza Task. [JythonTask.java](https://github.com/Quantiply/jython-samza/blob/master/src/main/java/com/quantiply/samza/JythonTask.java)
2. The Jython Samza task loads the Jython runtime using JSR-223. [code](https://github.com/Quantiply/jython-samza/blob/master/src/main/java/com/quantiply/samza/JythonTask.java#L21-L26)
3. Then the Jython bootstapping code is loaded. The bootstapping code requires two parameters : 
    * The APP_HOME directory - By convention is this one dir above the jar dir. [code](https://github.com/Quantiply/jython-samza/blob/master/src/main/java/com/quantiply/samza/JythonTask.java#L53-L55)
    * The Jython class to run - This class has to implement all the methods in the Task interface. The task interface is just an aggeregation of all Samza Task interfaces. [Task.java](https://github.com/Quantiply/jython-samza/blob/master/src/main/java/com/quantiply/samza/Task.java)
    
4. The bootstrapping code then sets up the jython classpath, loads the Jython class and assigns it to a global variable `com_quantiply_rico_entrypoint`. [bootstrap.py](https://github.com/Quantiply/metrics-jython/blob/master/lib/rico/bootstrap.py)

5. Finally, the Java Task gets the Jython task by accessing the global Jython object `com_quantiply_rico_entrypoint` and assiging it to a Task variable. [code](https://github.com/Quantiply/jython-samza/blob/master/src/main/java/com/quantiply/samza/JythonTask.java#L46-L50)

6. From this point on, the Jython class is treated as a Java class implementing the Task interface.


Rico tool
---
    
    rico local [-d] <name> [--env=<env>] [--full]
    rico samza <name> [--env=<env>]
    rico yarn  <name> [--env=<env>] [--http=<http_url>] [--build-only]
    rico build jar

-
    
    rico install-deps

Pip fix :
http://stackoverflow.com/questions/24257803/distutilsoptionerror-must-supply-either-home-or-prefix-exec-prefix-not-both



Directory structure
---
The processors are in `app` dir.


```
.
├── app
├── bin
│   └── rico
├── config
├── data
├── lib
├── logs
└── tmp
```

Let's look at it piece by piece: (*Outdated*)

- **app**: The directory with all the action. It's where you define:
    - **processors**: Your fundamental operations or "verbs", which are passed records and parse, filter, augment, normalize, or split them.
- **config**: Where you place all application configuration for all environments
    - **settings.yml**: Defines application-wide settings.
- **data**: Holds sample data in flat files. You'll develop and test your application using this data.
- **package.json**: Dependency management using NPM.
- **lib**: Holds any code you want to use in your application but that isn't "part of" your application (like vendored libraries, &c.).
- **log**: A good place to stash logs.
- **test**: Holds all your unit tests.
- **tmp**: A good place to stash temporary files.
