package com.pingan.examine

import java.util.Timer
import com.alibaba.fastjson.{JSON, JSONObject}
import com.pingan.examine.start.ConfigFactory
import com.pingan.examine.utils.UpdateHdfsTask
import jodd.util.StringUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 数据获取类
  * Created by Administrator on 2017/12/12.
  * Changed by Administrator on 2017/12/13.
  */
class DataInput {

  private val dataFeedback:DataFeedback = new DataFeedback()

  /**
    * 从kafka获取医疗机构要审核的报销信息
    */
  def getDataForKafka(): Unit ={
    ConfigFactory.initConfig()
    val conf = new SparkConf().setMaster(ConfigFactory.sparkstreammaster).setAppName(ConfigFactory.sparkstreamname)
    //val sparkContext = new SparkContext(conf)
    //如果既需要SparkContext，也需要StreamingContext，就需要传入sparkContext而不是conf，来初始化StreamingContext，解决sparkContext唯一性问题
    val streamingContext = new StreamingContext(conf,Seconds(ConfigFactory.sparkseconds))
    val sparkSession = SparkSession.builder().master(ConfigFactory.sparksqlmaster).appName(ConfigFactory.sparksqlname)
    val jsonSession:SparkSession = sparkSession.getOrCreate()
    //正式运行
    /*jsonSession.read.json("hdfs://examine/hisd504")
    val hisd504DataFrame:DataFrame = jsonSession.read.json("hdfs://master1.hadoop:9000/examine/hisd504")
    val hisd506DataFrame:DataFrame = jsonSession.read.json("hdfs://master1.hadoop:9000/examine/d506")
    hisd504DataFrame.createOrReplaceTempView("hisd504table")
    hisd506DataFrame.createOrReplaceTempView("d506table")*/



    /*接收来自kafka的数据*/
    val rmessageRdd:ReceiverInputDStream[(String,String)] = KafkaUtils.createStream(streamingContext,ConfigFactory.kafkazookeeper
    ,"user-behavior-topic-message-consumer-group",Map("reimbursement-message" -> 1),StorageLevel.MEMORY_ONLY)

    /**
      * foreachRdd中的函数操作可以循环
      */
    rmessageRdd.foreachRDD(rdd =>{
      rdd.persist()
      val count = rdd.count()
      println("本次处理数据数量："+count)
      if(count > 0){
        getDataFrameForDStream(rdd,sparkSession,jsonSession)
      }
    })
    startTimeTask()
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  /**
    * 处理单个DStream中的RDD
    * @param rdd 本次要处理的RDD
    * @param sparkSession SparkSession.Builder
    */
  private def getDataFrameForDStream(rdd:RDD[(String,String)],sparkSession:SparkSession.Builder,jsonSession:SparkSession):Unit = {
    val rdds:List[RDD[String]] = getDataFrame(rdd)
    val d504DataFrame:DataFrame = jsonSession.read.json(rdds(0))  //d504数据
    val d505DataFrame:DataFrame = jsonSession.read.json(rdds(1))  //d505数据
    d505DataFrame.persist()
    d504DataFrame.createOrReplaceTempView("d504table")
    d505DataFrame.createOrReplaceTempView("d505table")
    val newD504Rdd:RDD[String] = mainExamine(jsonSession,rdds(0))
    if(newD504Rdd == null){
      println("跳过第二次过滤，没有要处理的数据")
    }else{
      val newd504DataFrame:DataFrame = jsonSession.read.json(newD504Rdd)
      newd504DataFrame.createOrReplaceTempView("d504table")
      val endAdoptRdd:RDD[String] = repeatExamine(jsonSession,newD504Rdd)
      dataFeedback.handleResultData(rdds(0),endAdoptRdd)
    }

  }

  /**
    * 把原始的RDD[(String,String)]转化为RDD[String]，RDD中每行是一个完整的bean的json
    * @param rdd 原始的RDD
    * @return List中第一个元素为D504表rdd，第二个为D505表rdd
    */
  private def getDataFrame(rdd:RDD[(String,String)]):List[RDD[String]] = {
    val d504Rdd:RDD[String] = rdd.map{case(key,value) =>
      val beanjson:JSONObject = JSON.parseObject(value)
      beanjson.get("d504Bean").toString()
    }
    val d505Rdd:RDD[String] = rdd.flatMap{case(key,value) =>
      val beanjson:JSONObject = JSON.parseObject(value)
      var d505BeanList = beanjson.get("d505BeanList").toString
      d505BeanList = d505BeanList.substring(1,d505BeanList.length-1)
      val d505Array = StringUtil.split(d505BeanList,"},{")
      d505Array(0) = d505Array(0)+"}"
      d505Array(d505Array.length - 1) = d505Array(d505Array.length - 1) + "}"
      if(d505Array.length > 2){
        val cnt = 1;
        for(cnt <- 1 to d505Array.length - 1){
          d505Array(cnt) = "{" + d505Array(cnt) +"}"
        }
      }
      d505Array
    }
    List(d504Rdd,d505Rdd)
  }

  /**
    * 住院主审核模块
    * @param jsonSessin SparkSession
    * @param d504Rdd D504表的rdd
    * @return 过滤之后通过审核的新的d504rdd
    */
  private def mainExamine(jsonSessin:SparkSession,d504Rdd:RDD[String]):RDD[String] = {
    val sql =
      """select t1.d504_01,count(t1.d504_01)as cnt from d504table t1,d505table t2
        |where t1.d504_13<=14
        |and t2.e505_02='0'
        |and t1.d504_01=t2.d505_01
        |and t1.d504_12=t2.d505_13
        |group by t1.d504_01 having cnt<=15
      """.stripMargin
    /*
    """
        |select t1.d504_01,count(t1.d504_01) as cnt from d504table t1,d505table t2
        |where t1.d504_13 <= 14
        |and t2.e505_02 = 0
        |and t1.d504_01 = t2.d505_01
        |and t1.d504_12 = t2.d505_13
        |group by t1.d504_01
        |having cnt <= 15
      """.stripMargin
      */
    val rows:DataFrame = jsonSessin.sql(sql)
    println("======================================第一次过滤结果===================================")
    rows.show(1000);
    println("======================================================================================")
    val rowArray:Array[Row] = rows.collect()
    var adoptList:List[String] = List()
    for(cnt <- 0 until rowArray.length){
      adoptList = adoptList ++ List(rowArray(cnt).getString(0))
    }
    if(adoptList.size < 1){
      return null;
    }else{
      val newd504rdd:RDD[String] = d504Rdd.filter(line =>{
        val d504id = JSON.parseObject(line).get("d504_01").toString
        if(adoptList.contains(d504id)) true else false
      })
      newd504rdd
    }

  }

  /**
    * 重复住院过滤
    * @param jsonSession SparkSession
    * @param newD504Rdd 第一次过滤之后剩余的符合条件的记录集合
    * @return 第二次过滤之后剩余的复合条件的记录集合
    */
  private def repeatExamine(jsonSession:SparkSession,newD504Rdd:RDD[String]):RDD[String] = {
    val hisd504DataFrame:DataFrame = jsonSession.read.json("/examine/hisd504.txt")
    val d506DataFrame:DataFrame = jsonSession.read.json("/examine/d506.txt")
    hisd504DataFrame.createOrReplaceTempView("hisd504table")
    d506DataFrame.createOrReplaceTempView("d506table")
    val sql =
      """
        |select distinct t1.d504_01 from d504table t1,d505table t2,hisd504table t3,d506table t4
        |where t1.d504_01 = t2.d505_01 and t1.d504_02 = t3.d504_02 and t1.d504_02 = t4.e506_01
        |and (
        |(t1.d504_14 = t3.d504_14 and CAST(UNIX_TIMESTAMP(t1.d504_11,'dd-MM-yyyy') AS LONG) - CAST(UNIX_TIMESTAMP(t3.d504_12,'dd-MM-yyyy') AS LONG) <= 604800000)
        |or (t1.d504_21 = t3.d504_21 and (t1.d504_20 = '0' or t1.d504_20 = '') and CAST(UNIX_TIMESTAMP(t1.d504_11,'dd-MM-yyyy') AS LONG) - CAST(UNIX_TIMESTAMP(t3.d504_12,'dd-MM-yyyy') AS LONG) <= 604800000 and t1.d504_14 != t3.d504_14)
        |or((select sum(d506_24) from d506table where e506_01 = t1.d504_02 group by e506_01) + (select sum(e505_06) from d505table where d505_01 = t1.d504_01 group by d505_01) > 300000)
        |or t4.d506_25 != 0
        |)
      """.stripMargin

    val rows:DataFrame = jsonSession.sql(sql)
    println("======================================第二次过滤结果===================================")
    rows.show(1000);
    println("======================================================================================")
    val rowArray:Array[Row] = rows.collect()
    var adoptList:List[String] = List()
    for(cnt <- 0 until rowArray.length){
      adoptList = adoptList ++ List(rowArray(cnt).getString(0))
    }

    val endd504rdd:RDD[String] = newD504Rdd.filter(line => {
      val d504id = JSON.parseObject(line).get("d504_01").toString()
      if(adoptList.contains(d504id)) false else true
    })
    endd504rdd
  }

  private def startTimeTask(): Unit ={
    val timer = new Timer()
    timer.schedule(new UpdateHdfsTask(),0,1000*60)
  }

}
