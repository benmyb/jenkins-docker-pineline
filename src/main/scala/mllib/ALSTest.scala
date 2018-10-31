package mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object ALSTest extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("ALSTest").setMaster("local")
  val sc = new SparkContext(conf)

  //读取数据
  val data = sc.textFile("file:///soft/spark/data/mllib/als/test.data")
  val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
    Rating(user.toInt, item.toInt, rate.toDouble)
  })

  // Build the recommendation model using ALS
  val rank = 10
  val numIterations = 10
  val model = ALS.train(ratings, rank, numIterations, 0.01)

  // Evaluate the model on rating data
  val usersProducts = ratings.map { case Rating(user, product, rate) =>
    (user, product)
  }
  val predictions =
    model.predict(usersProducts).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }
  val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
    ((user, product), rate)
  }.join(predictions)
  val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
    val err = (r1 - r2)
    err * err
  }.mean()
  println(s"Mean Squared Error = $MSE")
}
