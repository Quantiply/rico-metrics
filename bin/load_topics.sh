DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ROOT_DIR=${DIR}/..

cat ${ROOT_DIR}/data/samza-metrics.json | ${ROOT_DIR}/deploy/confluent/bin/kafka-console-producer \
              --broker-list localhost:9092 --topic sys.samza_metrics \
              --compression-codec lz4 --new-producer

cat ${ROOT_DIR}/data/druid-metrics.json | ${ROOT_DIR}/deploy/confluent/bin/kafka-console-producer \
              --broker-list localhost:9092 --topic sys.druid_metrics \
              --compression-codec lz4 --new-producer
