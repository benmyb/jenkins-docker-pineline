package ml

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

object Kdd99 extends App {

  val conf = new SparkConf().setAppName("DecisionTree").setMaster("local[16]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val spark = SparkSession.builder().appName("Kdd99").config("example", "some-value").getOrCreate()


  //读取数据并给数据添加表头
  val data = spark.read.csv("/user/Tian/data/kddcup.data")

  val df = data.toDF("duration", "protocol_type", "service", "flag", "src_bytes", "dst_bytes", "land",
    "wrong_fragment", "urgent", "hot", "num_failed_logins", "logged_in", "num_compromised", "root_shell",
    "su_attempted", "num_root", "num_file_creations", "num_shells", "num_access_files", "num_outbound_cmds",
    "is_host_login", "is_guest_login", "count", "srv_count", "serror_rate", "srv_serror_rate", "rerror_rate",
    "srv_rerror_rate", "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate", "dst_host_count",
    "dst_host_srv_count", "dst_host_same_srv_rate", "dst_host_diff_srv_rate", "dst_host_same_src_port_rate",
    "dst_host_srv_diff_host_rate", "dst_host_serror_rate", "dst_host_srv_serror_rate", "dst_host_rerror_rate",
    "dst_host_srv_rerror_rate", "label")

  //修改"protocol_type"这一列为数值型
  val indexer_2 = new StringIndexer().setInputCol("protocol_type").setOutputCol("protocol_typeIndex")
  val indexed_2 = indexer_2.fit(df).transform(df)

  //修改"service"这一列为数值型
  val indexer_3 = new StringIndexer().setInputCol("service").setOutputCol("serviceIndex")
  val indexed_3 = indexer_3.fit(indexed_2).transform(indexed_2)

  //修改"flag"这一列为数值型
  val indexer_4 = new StringIndexer().setInputCol("flag").setOutputCol("flagIndex")
  val indexed_4 = indexer_4.fit(indexed_3).transform(indexed_3)

  //修改"label"这一列为数值型
  val indexer_final = new StringIndexer().setInputCol("label").setOutputCol("labelIndex")
  val indexed_df = indexer_final.fit(indexed_4).transform(indexed_4)

  //删除原有的类别列
  val df_final = indexed_df.drop("protocol_type").drop("service")
    .drop("flag").drop("label")

  //合并前41列为features
  val assembler = new VectorAssembler().setInputCols(Array("duration", "src_bytes", "dst_bytes", "land",
    "wrong_fragment", "urgent", "hot", "num_failed_logins", "logged_in", "num_compromised", "root_shell",
    "su_attempted", "num_root", "num_file_creations", "num_shells", "num_access_files", "num_outbound_cmds",
    "is_host_login", "is_guest_login", "count", "srv_count", "serror_rate", "srv_serror_rate", "rerror_rate",
    "srv_rerror_rate", "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate", "dst_host_count",
    "dst_host_srv_count", "dst_host_same_srv_rate", "dst_host_diff_srv_rate", "dst_host_same_src_port_rate",
    "dst_host_srv_diff_host_rate", "dst_host_serror_rate", "dst_host_srv_serror_rate", "dst_host_rerror_rate",
    "dst_host_srv_rerror_rate", "protocol_typeIndex", "serviceIndex", "flagIndex")).setOutputCol("features")

  //转换为double类型
  val cols = df_final.columns.map(f => col(f).cast(DoubleType))
  var data_fea: DataFrame = assembler.transform(df_final.select(cols: _*))

  //删除前41列，只留labelIndex和features这两列（data_fea.columns.length = 43）
  val colNames = data_fea.columns
  var dataset = data_fea.drop(colNames(0))

  for (colId <- 0 to 40) {
    dataset = dataset.drop(colNames(colId))
  }

  dataset = dataset.withColumnRenamed("labelIndex", "label")

  /*
  组装
   */
  //把数据随机分成训练集合测试集
  val Array(trainingData, testData) = dataset.randomSplit(Array(0.7, 0.3))

  //建立特征索引
  val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(dataset)

  //创建决策树模型
  val decisionTree = new DecisionTreeClassifier().setLabelCol("label")
    .setFeaturesCol("indexedFeatures").setImpurity("entropy").setMaxBins(100).setMaxDepth(5).setMinInfoGain(0.01)
  println("创建决策树模型...")

  //配置流水线
  val dtPipline = new Pipeline().setStages(Array(featureIndexer, decisionTree))
  println("配置流水线...")

  /*
  模型优化
   */
  //配置网格参数
  val dtParamGrid = new ParamGridBuilder()
    .addGrid(decisionTree.maxDepth, Array(3, 5, 7))
    .build()

  //实例化交叉验证模型
  val evaluator = new BinaryClassificationEvaluator()
  val dtCV = new CrossValidator()
    .setEstimator(dtPipline)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(dtParamGrid)
    .setNumFolds(2)

  //通过交叉验证模型，获取最优参数集，并测试模型
  val dtCVModel = dtCV.fit(trainingData)
  val dtPrediction = dtCVModel.transform(testData)

  //查看决策树匹配模型的参数
  val dtBestModel = dtCVModel.bestModel.asInstanceOf[PipelineModel]
  val dtModel = dtBestModel.stages(1).asInstanceOf[DecisionTreeClassificationModel]
  print("决策树模型深度：")
  println(dtModel.getMaxDepth)

  //统计预测正确率
  //t:决策树预测值的数组
  //label:测试集的标签值数组
  //count:测试集的数量
  val (t, label, count) = (dtPrediction.select("prediction").collect,
    testData.select("label").collect(),
    testData.count().toInt)
  var dt = 0
  for (i <- 0 to count - 1) {
    if (t(i) == label(i)) {
      dt += 1
    }
  }
  //打印正确率
  println("正确率：" + 1.0 * dt / count)

}
