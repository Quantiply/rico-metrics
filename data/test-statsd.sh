#!/bin/bash
IP=$1
while true
do
    echo -n "example.statsd.counter.changed:$(((RANDOM % 11) + 1))|c" | nc $IP 8125
done
