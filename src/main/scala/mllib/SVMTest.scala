package mllib

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object SVMTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SVMTest").setMaster("spark://sict136:7077")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //读取并创建数据
    val data = sc.textFile("file:///soft/spark/data/mllib/sample_svm_data.txt")   //使用spark自带的测试数据
    val parsedData = data.map { line =>
      val parts = line.split(' ')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts.tail.map(x => x.toDouble)))
    }
    //设置迭代次数并进行训练
    val numIterations = 50
    //训练模型
    val model = SVMWithSGD.train(parsedData, numIterations)
    //预测并统计分类错误的样本
    val labelAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedData.count()
    println("Training Error = " + trainErr)
  }

}
