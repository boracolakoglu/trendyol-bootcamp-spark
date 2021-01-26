package com.trendyol.bootcamp.homework

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ProductMergerJob {

  def main(args: Array[String]): Unit = {

    /** Read both json files as sql
     *  Merge them togeher
     *  Select id, name, category, brand, color, price, max(timestamp)
     *  from mergedTable
     *  gruop by id 
     */
  
    import spark.implicits._

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark HomeWork")
      .getOrCreate()

    val productsSchema = Encoders.product[Product].schema
    val products = spark.read
    .schema(productsSchema)
    .json("data/homework/initial_data")
    .as[Product]
      

    val changes = spark.read
    .schema(productsSchema)
    .json("data/homework/cdc_data")

    val merged = ???

    /**
    * Find the latest version of each product in every run, and save it as snapshot.
    *
    * Product data stored under the data/homework folder.
    * Read data/homework/initial_data.json for the first run.
    * Read data/homework/cdc_data.json for the nex runs.
    *
    * Save results as json, parquet or etc.
    *
    * Note: You can use SQL, dataframe or dataset APIs, but type safe implementation is recommended.
    */

  }

}
