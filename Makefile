#!/bin/bash

.PHONY: help

help: ## Displays target and their Function.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

TOPIC?=deafultTestTopic
FILE?=none
REPEAT?=false

build-spark-image: ## Builds the spark-image from Dockerfile
	docker build -t rj1234/pyspark_development:latest .

run-spark-container: ## Build and run the spark-container
	docker run -v /home/rj/Playground/bigdata:/mnt/host/Playground/bigdata \
			   -w /mnt/host -ti \
			   -p 8888:8888 \
			   -p 4040:4040 \
			   -p 9092:9092 \
			   -p 2181:2181 \
			   -P \
			   --env JUPYTER_TOKEN=root \
			   --name pyspark_develeopment_container \
			   rj1234/pyspark_development:latest

show-all-containers: ## Shows all available docker containers
	docker ps -a

show-running-containers: ## Shows the running containers
	docker ps

show-images: ## Shows all docker images locally
	docker images

start-spark-container: ## Start the spark-container
	docker start pyspark_develeopment_container
	docker exec -t -i pyspark_develeopment_container bash

stop-spark-container:	## Stop the spark-container
	docker stop pyspark_develeopment_container

remove-spark-containers:	## Remove the spark-container
	docker stop pyspark_develeopment_container
	docker rm pyspark_develeopment_container

remove-all-containers:	## ***REMOVES*** ALL CONTAINERS
	docker rm $$(docker ps -a -q)

kz-start-servers:	## Start zookeeper and kafka servers as daemons
	$$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $$KAFKA_HOME/config/zookeeper.properties
	$$KAFKA_HOME/bin/kafka-server-start.sh -daemon $$KAFKA_HOME/config/server.properties

kz-stop:	## Stop kafka and zookeeper servers
	$$KAFKA_HOME/bin/kafka-server-stop.sh
	$$KAFKA_HOME/bin/zookeeper-server-stop.sh

list-kafka-topics:	## List all the kafka topics
	$$KAFKA_HOME/bin/kafka-topics.sh --zookeeper $$ZK_HOSTS --list

create-topic:	## Create a new topic
	@$$KAFKA_HOME/bin/kafka-topics.sh --zookeeper $$ZK_HOSTS --create --topic $$TOPIC --replication-factor 1 --partitions 1

delete-topic: ## delete a kafka topic
	@$$KAFKA_HOME/bin/kafka-topics.sh --zookeeper $$ZK_HOSTS --delete --topic $$TOPIC

start-producer: ## Starts a producer and sends the console input to the topic	
	if [ $$FILE = none ] ; then \
		$$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list $$KAFKA_BROKERS --topic $$TOPIC;\
	else \
		if [ $$REPEAT = false || ! $$REPEAT ]; then \
			cat $$FILE | $$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list $$KAFKA_BROKERS --topic $$TOPIC ;\
		else \
			if [ $$REPEAT = true ] ; then\
				while [ 1 = 1 ]; do \
					cat $$FILE | $$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list $$KAFKA_BROKERS --topic $$TOPIC ;\
				done \
			else \
				echo "REPEAT only accepts 'true' or  'false' " ; \
			fi \
		fi \
	fi

push: ## Push to the docker hub
	docker push rj1234/pyspark_development:latest_$$(date +%Y%m%d%H%M%S)

clean: check_clean	## ***REMOVE***  ALL CONTAINERS AND IMAGES!!!
	docker rm $$(docker ps -a -q)
	docker rmi $$(docker images -q)

check_clean:
	@echo -n "Are you sure? [y/N] " && read ans && [ $${ans:-N} = y ]
