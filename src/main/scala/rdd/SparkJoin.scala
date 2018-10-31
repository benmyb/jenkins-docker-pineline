package rdd

import org.apache.spark.{SparkConf, SparkContext}

object SparkJoin {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("usage is WordCount<rating> <movie> <output>")
      return
    }
    val conf = new SparkConf().setAppName("SparkJoin").setMaster("local")
    val sc = new SparkContext(conf)
    //加载评分文件
    val textFile = sc.textFile((args(0)))
    //抽取（movieId,rating)
    val rating = textFile.map(line => {
      val fileds = line.split(",")
      (fileds(1).toInt, fileds(2).toDouble)
    })
    //得到（movieId,ave_rating)
    val movieScores = rating.groupByKey().map(data => {
      val avg = data._2.sum / data._2.size
      (data._1, avg)
    })

    //加载电影文件
    val movies = sc.textFile(args(1))
    val movieskey = movies.map(line => {
      val fileds = line.split(",")
      (fileds(0).toInt, fileds(1)) //(movieId,movieName)
    }).keyBy(tup => tup._1)

    //通过连接操作，得到<movie,avg_rating,movieName>
    val result = movieScores.keyBy(tup => tup._1).join(movieskey)
      .filter(f => f._2._1._2 > 4.0)
      .map(f => (f._1, f._2._1._2, f._2._2._2))
    result.saveAsTextFile(args(2))
  }

}
