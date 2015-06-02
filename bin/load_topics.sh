DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ROOT_DIR=${DIR}/..

data/test-json.sh 100 | ${ROOT_DIR}/deploy/confluent/bin/kafka-console-producer \
              --broker-list localhost:9092 --topic echo.in \
              --compression-codec lz4 --new-producer

# ${ROOT_DIR}/deploy/confluent/bin/kafka-console-producer \
#               --broker-list localhost:9092 --topic echo.in \
#               --compression-codec lz4 --new-producer < ${ROOT_DIR}/data/10M.json

# ${ROOT_DIR}/deploy/confluent/bin/kafka-avro-console-producer \
#              --broker-list localhost:9092 --topic echo-avro.in \
#              --property schema.registry.url=http://localhost:9081 \
#              --compression-codec lz4 --new-producer \
#              --property value.schema="`cat config/schema/avro/mumbo_jumbo.avsc`" < ${ROOT_DIR}/data/10M.json
#