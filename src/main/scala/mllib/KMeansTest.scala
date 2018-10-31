package mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object KMeansTest extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("Kmeans").setMaster("local")
  val sc = new SparkContext(conf)

  //读取数据
  val data = sc.textFile("file:///soft/spark/data/mllib/kmeans_data.txt")
  val parsedData = data.map(s => Vectors.dense(s.split(" ").map(_.toDouble)))

  //训练模型
  val numClusters = 2
  val numIterations = 15
  val model = KMeans.train(parsedData, numClusters, numIterations)
  println("Cluster centers")
  for (c <- model.clusterCenters) {
    println(" " + c.toString)
  }

  //评估模型
  val cost = model.computeCost(parsedData)
  println("Within the sum of squared Error : " + cost)

  //使用模型对单条特征进行预测
  println("predict:" + model.predict(Vectors.dense("0.1,0.1,0.3".split(",").map(_.toDouble))))
  println("predict:" + model.predict(Vectors.dense("7.1,8.2,9.3".split(",").map(_.toDouble))))

}
