package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import com.ibm.icu.text.SimpleDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.util
import scala.collection.mutable
import scala.util.control.Breaks._


object AlertApp {
  def main(args: Array[String]): Unit = {

    //创建spark配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")

    //创建入口
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))


    //消费Kafka数据
    val KafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    //将数据转化为样例类并补全字段（将数据转为KV结构）
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midTOEventLogDStream: DStream[(String, EventLog)] = KafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

        //补全字段
        val times: String = sdf.format(eventLog.ts)
        eventLog.logDate = times.split(" ")(0)
        eventLog.logHour = times.split(" ")(1)

        //改变返回类型
        (eventLog.mid, eventLog)

      })
    })

    //开窗（5min内）
    val windowMidToLogDStream: DStream[(String, EventLog)] = midTOEventLogDStream.window(Minutes(5))

    //分组聚合（同一设备）
    val midToIterLogDStream: DStream[(String, Iterable[EventLog])] = windowMidToLogDStream.groupByKey()

    //根据条件进行筛选 返回疑似(未完全过滤)预警日志
    /**
     * 三次及以上用不同账号登录并领取优惠劵，（过滤出领取优惠券行为，把相应的uid存放到set集合中，并统计个数）
     * 并且过程中没有浏览商品。（如果用户有浏览商品的行为，则不作分析）
     */
    val boolToCouponAlertDStream: DStream[(Boolean, CouponAlertInfo)] = midToIterLogDStream.mapPartitions(partition => {
      partition.map { case (mid, iter) =>

        //创建用来存放领优惠券的用户id
        val uids: util.HashSet[String] = new util.HashSet[String]()
        //闯将用来存放领优惠券所涉及的商品id
        val itemIds: util.HashSet[String] = new util.HashSet[String]()
        //创建用来存放用户所涉及事件的集合
        val events: util.ArrayList[String] = new util.ArrayList[String]()

        //创建一个标志位，用来判断用户是否有浏览商品行为
        var bool: Boolean = true

        breakable {
          iter.foreach(log => {
            //添加用户涉及行为
            events.add(log.evid)
            if ("clickItem".equals(log.evid)) {
              //浏览商品行为  进来就是有浏览商品的行为就中断循环
              bool = false
              break()
            } else if ("coupon".equals(log.evid)) {
              //添加用户和商品id
              uids.add(log.uid)
              itemIds.add(log.itemid)
            }
          })
        }
        //因为此时无法过滤掉uids<3和事件里包含浏览商品的事件 ，所以我们设计成KV结构，以便接下来的过滤
        (uids.size() >= 3 & bool, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    })

    //生成预警日志
    //    val value: DStream[(Boolean, CouponAlertInfo)] = boolToCouponAlertDStream.filter(_._1)
    //    val couponAlertInfoDStream: DStream[CouponAlertInfo] = value.map(_._2)
    //写成一行
    val couponAlertInfoDStream: DStream[CouponAlertInfo] = boolToCouponAlertDStream.filter(_._1).map(_._2)

    couponAlertInfoDStream.print()

    //将预警日志写入ES并去重
    couponAlertInfoDStream.foreachRDD(rdd =>{
        rdd.foreachPartition(partition => {
          val list: List[(String, CouponAlertInfo)] = partition.toList.map(log => {
            (log.mid + log.ts / 1000 / 60, log)
          })

          MyEsUtil.insertBulk(GmallConstants.ES_ALERT_INDEXNAME+ "0625",list)
        })
    })

    //开启连接
    ssc.start()
    ssc.awaitTermination()


  }
}
