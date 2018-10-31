package mllib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint

object NaiveBayesTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("NaiveBayesTest").setMaster("spark://sict136:7077")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //读取并创建训练数据
    val data = sc.textFile("file:///soft/spark/mycode/data/nb_traindata.txt")
    val trainData = data.map { line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }
    //读取测试数据
    val testData = sc.textFile("file:///soft/spark/mycode/data/nb_testdata.txt").map {
      line =>
        val parts = line.split(" ")
        Vectors.dense(parts.map(_.toDouble))
    }
    //训练模型
    val model = NaiveBayes.train(trainData, lambda = 1.0)

    //使用模型对单条特征进行预测
    println("predict:" + model.predict(Vectors.dense("0,0,3".split(",").map(_.toDouble))))

    //使用模型对批量特征进行预测，并将结果保存至文件夹
    val result = testData.map { line => {
      val prediction = model.predict(line)
      line + "belongs to class " + prediction
    }
    }.saveAsTextFile("file:///soft/spark/mycode/data/result.txt")
  }

}
