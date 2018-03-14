package com.pingan.examine

import java.io.{File, FileOutputStream, OutputStreamWriter}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.pingan.examine.start.ConfigFactory
import com.pingan.examine.utils.KafkaUtil
import org.apache.spark.rdd.RDD

/**
 * 数据反馈类
  * 主要用于把数据返回给调用接口
  * 并把数据做本系统的持久化保存
 * Created by leoyang on 2017/12/15.
 */
class DataFeedback {

  /**
    * 处理过滤后的结果集
    * @param sourceData 传入的原始数据
    * @param adoptData 通过过滤的数据
    */
  def handleResultData(sourceData:RDD[String],adoptData:RDD[String]):Unit = {
    var adoptArray:Array[String] = null
    var noAdoptArray:Array[String] = null
    if(adoptData==null){
      sourceData.foreach(rdd=>{
        val d504Bean:JSONObject = JSON.parseObject(rdd)
        println("发送审核未通过数据到kafka")
        KafkaUtil.sendDataToKafka(d504Bean.get("d504_14").toString,d504Bean.get("d504_01").toString,"error")
      })
      noAdoptArray = sourceData.collect()
    }else{
      val noAdoptData:RDD[String] = sourceData.subtract(adoptData)
      adoptData.foreach(rdd=>{
        val d504Bean:JSONObject = JSON.parseObject(rdd)
        println("发送审核通过数据到kafka")
        KafkaUtil.sendDataToKafka(d504Bean.get("d504_14").toString,d504Bean.get("d504_01").toString,"success")
      })
      noAdoptData.foreach(rdd=>{
        val d504Bean:JSONObject = JSON.parseObject(rdd)
        println("发送审核未通过数据到kafka")
        KafkaUtil.sendDataToKafka(d504Bean.get("d504_14").toString,d504Bean.get("d504_01").toString,"error")
      })
      if(noAdoptData != null){
        adoptArray = adoptData.collect()
        noAdoptArray = noAdoptData.collect()
      }
    }
    val time = System.currentTimeMillis()
    writeToLocal(ConfigFactory.localpath+File.separator+"adopt"+File.separator+time+".txt",adoptArray)
    writeToLocal(ConfigFactory.localpath+File.separator+"noadopt"+File.separator+time+".txt",noAdoptArray)
  }

  /**
    * 保存内容到文件
    * @param path 文件完整路径
    * @param dataArray dataArray
    */
  private def writeToLocal(path:String,dataArray:Array[String]):Unit = {
    if (dataArray == null || dataArray.length<1){
      return
    }
    println(s"保存文件：${path}")
    val file:File = new File(path)
    val parentPath = file.getParentFile
    if(!parentPath.exists()){
      file.getParentFile.mkdirs()
    }
    if(file.createNewFile()){
      val out = new FileOutputStream(file)
      val writer = new OutputStreamWriter(out,"UTF-8")
      for (cnt <- 0 until dataArray.length){
        writer.write(dataArray(cnt)+"\r\n")
      }
      writer.flush()
      writer.close()
      out.close()
    }
  }
}
