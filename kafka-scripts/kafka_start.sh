#!/bin/bash

if [ "$1" == "" ]; then
  echo "Please supply topic name"
  exit 1
fi

if [ "$2" == "" ]; then
  echo "Please supply file to stream"
  exit 1
fi

nohup ~/kafka/bin/zookeeper-server-start.sh config/zookeeper.properties > /dev/null 2>&1
nohup ~/kafka/bin/kafka-server-start.sh config/server.properties > /dev/null 2>&1

echo $1
echo $2

nohup ~/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic $1 < "$2" >/dev/null 2>&1
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$1" --from-beginning
