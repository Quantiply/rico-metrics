DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

${DIR}/../deploy/confluent/bin/kafka-topics --zookeeper localhost:2181 --topic sys.samza_metrics --create --partitions 1 --replication-factor 1
${DIR}/../deploy/confluent/bin/kafka-topics --zookeeper localhost:2181 --topic echo.in --create --partitions 1 --replication-factor 1
${DIR}/../deploy/confluent/bin/kafka-topics --zookeeper localhost:2181 --topic echo.out --create --partitions 1 --replication-factor 1
${DIR}/../deploy/confluent/bin/kafka-topics --zookeeper localhost:2181 --topic echo-avro.in --create --partitions 1 --replication-factor 1
${DIR}/../deploy/confluent/bin/kafka-topics --zookeeper localhost:2181 --topic echo-avro.out --create --partitions 1 --replication-factor 1
