package com.barrelsofdata.sparkexamples

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Driver {

  val JOB_NAME: String = "Word Count"
  val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

  def run(spark: SparkSession, inputFilePath: String, outputFilePath: String): Unit = {
    import spark.implicits._

    val data: Dataset[String] = spark.read.textFile(inputFilePath)

    val words: Dataset[String] = data
      .map(WordCount.cleanData)
      .flatMap(WordCount.tokenize)
      .filter(_.nonEmpty)

    val wordFrequencies: DataFrame = words
      .map(WordCount.keyValueGenerator)
      .rdd.reduceByKey(_ + _)
      .toDF("word", "frequency")

//    val wordFrequencies: DataFrame = words
//      .groupBy(col("value")).count()
//      .toDF("word", "frequency")

    wordFrequencies
      .coalesce(1)
      .write
      .option("header","true")
      .csv(outputFilePath)

    LOG.info(s"Result successfully written to $outputFilePath")
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 2) {
      println("Invalid usage")
      println("Usage: spark-submit --master <local|yarn> spark-wordcount-1.0.jar /path/to/input/file.txt /path/to/output/directory")
      LOG.error(s"Invalid number of arguments, arguments given: [${args.mkString(",")}]")
      System.exit(1)
    }
    val spark: SparkSession = SparkSession.builder().appName(JOB_NAME).getOrCreate()

    run(spark, args(0), args(1))

  }

}
