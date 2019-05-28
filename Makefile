.PHONY: help

help: ## Displays target and their Function.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help


build-spark-image: ##	Builds the spark-image from Dockerfile
	docker build -t pyspark_develeopment .

run-spark-container: ##	Build and run the spark-container
	docker run -v /home/rj/Playground/bigdata:/mnt/host/Playground/bigdata \
			   -w /mnt/host -ti \
			   -p 8888:8888 \
			   --env JUPYTER_TOKEN=root \
			   --name pyspark_develeopment_container \
			   pyspark_develeopment

start-spark-container: ##	Start the spark-container
	docker start pyspark_develeopment_container
	docker exec -t -i pyspark_develeopment_container bash

stop-spark-container:	##	Stop the spark-container
	docker stop pyspark_develeopment_container

remove-all-containers:	##	Remove the spark-container
	docker rm $$(docker ps -a -q)

remove-all-containers:	##	***REMOVES*** ALL CONTAINERS
	docker rm $$(docker ps -a -q)

clean: check_clean	##	***REMOVE***  ALL CONTAINERS AND IMAGES!!!
	docker rm $$(docker ps -a -q)
	docker rmi $$(docker images -q)

check_clean:
	@echo -n "Are you sure? [y/N] " && read ans && [ $${ans:-N} = y ]
