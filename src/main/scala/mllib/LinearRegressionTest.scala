package mllib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

object LinearRegressionTest extends App {
  val conf = new SparkConf().setAppName("LinearRegressionTest").setMaster("spark://sict136:7077")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  //读取并创建训练数据
  val data = sc.textFile("file:///soft/spark/data/mllib/ridge-data/lpsa.data")
  val parsedData = data.map { line =>
    val parts = line.split(',')
    LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
  }.cache()

  // Building the model
  val numIterations = 100
  val stepSize = 0.1
  val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)

  // Evaluate model on training examples and compute training error
  val valuesAndPreds = parsedData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }
  val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
  println(s"training Mean Squared Error $MSE")

}
