package rdd

import org.apache.spark.sql.SparkSession

object RddToDataFrame {

  def main(args: Array[String]): Unit ={
     val spark = SparkSession.builder().appName("").config("", "").getOrCreate()
     spark.read.parquet()
  }

}
