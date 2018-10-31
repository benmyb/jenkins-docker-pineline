package mllib

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreeTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DecisionTreeTest").setMaster("spark://sict136:7077")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //读取并创建训练数据
    val data = MLUtils.loadLibSVMFile(sc, "file:///soft/spark/data/mllib/sample_libsvm_data.txt")
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainData, testData) = (splits(0), splits(1))

    //训练模型
    val numClasses = 2 //分类数量
    val categoricalFeaturesInfo = Map[Int, Int]() //用map存储类别特征及每个类别特征对应的数量
    val impurity = "gini" //纯度计算方法
    val maxDepth = 5 //树的最大高度
    val maxBins = 32 //用于分裂特征的最大划分数量
    val model = DecisionTree.trainClassifier(trainData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    //使用模型对批量特征进行预测
    val labeleIAndPreds = testData.map { point =>
      val prediction = model.predict((point.features))
      (point.label, prediction)
    }

    val testErr = labeleIAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)
  }

}
