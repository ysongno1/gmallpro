package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handle.DauHandle
import com.atguigu.utils.MyKafkaUtil
import java.util.Date
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3)) //每三秒一个批次 每个批次封装成一个RDD

    //3.消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //4.将数据转为样例类，补全字段
    //Driver端运行  Driver端去到Execute端需序列化 而SimpleDate的祖宗类实现了序列化接口
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

      /*
        mappatririon算子与map算子的区别
          mappatririon:一个分区一个线程并行执行（效率高），但是如果某个分区数据量太大，会导致oom,因为内部用的是集合（跌代器）来保存数据的（基于内存）
          map：将所有数据放到一个线程中执行（效率低）
       */
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        //补全两个字段 2021-10-30 15 Execute端执行
        val times: String = sdf.format(new Date(startUpLog.ts))
        //LogDate
        startUpLog.logDate = times.split(" ")(0)
        //LogHour
        startUpLog.logHour = times.split(" ")(1)

        startUpLog
      })
    })


    //5.做批次间去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandle.filterByRedis(startUpLogDStream,ssc.sparkContext)

    //6.做批次内去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandle.filterByGroup(filterByRedisDStream)

    //7.将去重后的结果（mid）保存在redis中
    DauHandle.saveToRedis(filterByGroupDStream)

    //8.将去重后的结构（明细数据 ）保存在hbase中
    filterByGroupDStream.foreachRDD(rdd =>{
      rdd.saveToPhoenix(
        "GMALLPRO_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })


    //4.启动ssc
    ssc.start()
    ssc.awaitTermination()
  }
}
