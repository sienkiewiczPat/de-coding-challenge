package com.truata.assignment.de

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.FileWriter
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{avg, col, count, max, min}
import org.slf4j.{Logger, LoggerFactory}

object Solver {

  val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val spark: SparkSession = getSparkSession

  def process(line: String): Unit = {
    line match {
      case "task-1-2a" => task1_2a
      case "task-1-2b" => task1_2b
      case "task-1-3" => task1_3
      case "task-2-2" => task2_2
      case "task-2-3" => task2_3
      case "task-2-4" => task2_4
      case "task-3-2" => task3_2
      case _ => throw new Exception("Error: invalid input")
    }
  }

  def task1_2a(implicit spark: SparkSession): Unit = {
    val input = readToRdd("groceries.csv")
    val allProducts = input.flatMap(line => line.split(","))
    val uniqueProducts = allProducts.distinct()
    uniqueProducts.saveAsTextFile("out/out_1_2a.txt")
  }

  def task1_2b(implicit spark: SparkSession): Unit = {
    val input = readToRdd("groceries.csv")
    val allProducts = input.flatMap(line => line.split(","))
    val allProductsCount = allProducts.count()
    val prepareDataToWrite = Seq("Count:", allProductsCount.toString)
    writeToFile(prepareDataToWrite, "out_1_2b.txt")
  }

  def task1_3(implicit spark: SparkSession): Unit = {
    val input = readToRdd("groceries.csv")
    val allProducts = input.flatMap(line => line.split(","))
    val productFrequencyPairs = allProducts.map(product => (product, 1))
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2))
      .sortBy(_._2, ascending = false)
      .take(5)
    val prepareDataToWrite = productFrequencyPairs.map(_.toString())
    writeToFile(prepareDataToWrite, "out_1_3.txt")
  }

  def task2_2(implicit spark: SparkSession): Unit = {
    val airbnbData = readToDf("sf-airbnb-clean.parquet")
    airbnbData
      .agg(
        min("price"),
        max("price"),
        count("*")
      ).toDF("min_price", "max_price", "row_count")
      .write.mode(SaveMode.Overwrite).csv("out/out_2_2.txt")
  }

  def task2_3(implicit spark: SparkSession): Unit = {
    val airbnbData = readToDf("sf-airbnb-clean.parquet")
    airbnbData
      .where(col("price") > 5000)
      .where(col("review_scores_value") === 10.0)
      .agg(
        avg("bathrooms"),
        avg("bedrooms")
      ).toDF("avg_bathrooms", "avg_bedrooms")
      .write.mode(SaveMode.Overwrite).csv("out/out_2_3.txt")
  }

  def task2_4(implicit spark: SparkSession): Unit = {
    val airbnbData = readToDf("sf-airbnb-clean.parquet")
    val airbnbDataFiltered =
      airbnbData
        .agg(
          min("price").alias("min_price"),
          max("review_scores_value").alias("highest_rating")
        )
    airbnbDataFiltered.join(airbnbData, airbnbDataFiltered("min_price") === airbnbData("price")
      && airbnbDataFiltered("highest_rating") === airbnbData("review_scores_value"), "inner")
      .select("accommodates")
      .write.mode(SaveMode.Overwrite).csv("out/out_2_4.txt")
  }

  def task3_2(implicit spark: SparkSession): Unit = {
    import org.apache.spark.ml.classification.LogisticRegression
    import org.apache.spark.ml.feature.VectorAssembler

    val columns = Array("sepal_length", "sepal_width", "petal_length", "petal_width", "class")
    val df = spark.read.option("inferSchema", value = true).csv("/tmp/iris.csv").toDF(columns: _*)

    val assembler = new VectorAssembler()
      .setInputCols(columns.dropRight(1))
      .setOutputCol("features")

    val dfTransformed = assembler.transform(df)
    val train, test = dfTransformed.randomSplit(Array(0.70, 0.30))

    val dfLabeled = new StringIndexer()
      .setInputCol("class")
      .setOutputCol("label")
      .fit(train.head)
      .transform(train.head)

    val lr = new LogisticRegression()
    val lrModel = lr.fit(dfLabeled)

    val predData = spark.createDataFrame(
      Seq((5.1, 3.5, 1.4, 0.2),
        (6.2, 3.4, 5.4, 2.3)),
    ).toDF("sepal_length", "sepal_width", "petal_length", "petal_width")

    val predDataPrepared = assembler.transform(predData)
    val prediction = lrModel.transform(predDataPrepared)
    prediction.show(10, truncate = false)

  }

  def readToRdd(inFileName: String): RDD[String] = spark.sparkContext.textFile(f"input/$inFileName")
  def readToDf(inFileName: String): DataFrame = spark.read.load(f"input/$inFileName")
  def writeToFile(data: Seq[String], outFileName: String): Unit = {
    val fw = new FileWriter(f"out/$outFileName")
    try {
      data.map(line => f"$line\n").foreach(fw.write)
    }
    finally fw.close()
  }
  def getSparkSession: SparkSession = SparkSession.builder().master("local[1]").getOrCreate()

}