package rdd

import org.apache.spark.{SparkConf, SparkContext}

object SecondarySortApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SecondarySortApp").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("file:///soft/spark/mycode/data/file1.txt", 1)
    val pairWithSortKey = lines.map(line => (new SecondarySortKey(
      line.split(" ")(0).toInt, line.split(" ")(1).toInt), line))
    val sorted = pairWithSortKey.sortByKey(false)
    val sortedResult = sorted.map(sortedLine => sortedLine._2)
    sortedResult.collect().foreach(println)

  }

}
