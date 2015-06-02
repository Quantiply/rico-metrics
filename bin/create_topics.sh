DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

${DIR}/../deploy/confluent/bin/kafka-topics --zookeeper localhost:2181 --topic sys.samza_metrics --create --partitions 1 --replication-factor 1
${DIR}/../deploy/confluent/bin/kafka-topics --zookeeper localhost:2181 --topic sys.druid_metrics --create --partitions 1 --replication-factor 1
${DIR}/../deploy/confluent/bin/kafka-topics --zookeeper localhost:2181 --topic sys.statsd --create --partitions 1 --replication-factor 1
