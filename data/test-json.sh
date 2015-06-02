#!/bin/bash
NUM_RECORDS=${1:=100}
for i in `seq $NUM_RECORDS`; do
    echo '{"headers": {"hi" : "there"}, "payload":{"mumbo": "jumbo", "id":' $i '}}'
    # sleep 1s
done
