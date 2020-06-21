# Spark Word Count with Unit Tests
This is a project detailing how to write word count program in Apache Spark along with unit test cases. The related blog post can be found at [https://www.barrelsofdata.com/spark-word-count-with-unit-tests](https://www.barrelsofdata.com/spark-word-count-with-unit-tests)

## Build instructions
From the root of the project execute the below commands
- To clear all compiled classes, build and log directories
```shell script
./gradlew clean
```
- To run tests
```shell script
./gradlew test
```
- To build jar
```shell script
./gradlew shadowJar
```
- All combined
```shell script
./gradlew clean test shadowJar
```

## Run
```shell script
spark-submit --master yarn --deploy-mode cluster build/libs/spark-wordcount-1.0.jar hdfs://path/to/input/file.txt hdfs://path/to/output/directory
```