package rdd

import org.apache.spark.{SparkConf, SparkContext}

object TopValue {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TopValue").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val lines = sc.textFile("hdfs://192.168.135.136:8020/user/Tian/data", 2)
    var num = 0
    val result = lines.filter(line => (line.trim().length > 0) && (line.split(",").length == 4))
      .map(_.split(",")(2))
      .map(x => (x.toInt, ""))
      .sortByKey(false)
      .map(x => x._1).take(5)
      .foreach(x => {
        num = num + 1
        println(num + "\t" + x)
      })
  }

}
