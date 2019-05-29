# Docker Image 
## Built for pyspark development
### Things thar are properly confugured (AFAIK)
- JAVA 8 OpenJDK
- Anaconda3
- Spark-2.4.3-bin-hadoop2.7

> pyspark is configured to use jupyter notebook

## Makefile options
- make build-spark-image > Builds the spark-image from Dockerfile
- make run-spark-container > Build and run the spark-container
- make start-spark-container > Start the spark-container
- make stop-spark-container > Stop the spark-container
- make remove-spark-container > ***Remove*** the spark-container
- make remove-all-containers > ***Remove*** the spark-container
- make push > Push to the docker hub
- make clean > ***REMOVE***  ALL CONTAINERS AND IMAGES!!!
