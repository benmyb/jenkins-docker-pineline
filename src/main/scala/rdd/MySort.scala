package rdd

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object MySort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MySort")
    val sc = new SparkContext(conf)
    val dataFile = "file:///soft/spark/mycode/data"
    val data = sc.textFile(dataFile)
    var index = 0
    val result = data.filter(_.trim().length > 0).map(n => (n.trim.toInt, "")).partitionBy(
      new HashPartitioner(1)).sortByKey().map(t => {
      index += 1
      (index, t._1)
    }
    )
    result.foreach(println)
  }

}
