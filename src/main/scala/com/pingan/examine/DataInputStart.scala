package com.pingan.examine

import com.pingan.examine.start.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Administrator on 2017/12/13.
  */
object DataInputStart {
  def main(args: Array[String]): Unit = {
    new DataInput().getDataForKafka()
  }

}
