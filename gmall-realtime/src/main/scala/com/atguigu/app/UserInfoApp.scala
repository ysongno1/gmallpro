package com.atguigu.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization


object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //创建Sparkconf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailAPP")

    //创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //消费Kafka上的数据
    val userInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

    //转换样例类(为了获取userInfo的id 拼凑redisKey) => 再转为Json字符串(以Json String的格式存入redis) => 在SaleDetailAPP类中又将Json字符串提取出来进行匹配
    /*
    //转换格式
    val userInfoDStream: DStream[UserInfo] = userInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
        userInfo
      })
    })

    //将userInfo数据写入Redis中
    userInfoDStream.foreachRDD(rdd =>{
      implicit val formats = org.json4s.DefaultFormats
      rdd.foreachPartition(partition => {
        //创建redis连接
        val jedis: Jedis = new Jedis("hadoop102", 6379)
        partition.foreach(record => {
          //userInfo redisKey
          val userInfoRediskey: String = "UserInfo:"+record.id
          val userInfoJsonStr: String = Serialization.write(record)
          jedis.set(userInfoRediskey,userInfoJsonStr)
        })
        jedis.close()
      })
    })
     */

    //从Kafka中出来的的Value直接就是字符串
    userInfoKafkaDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(partition => {
        //创建redis连接
        val jedis: Jedis = new Jedis("hadoop102", 6379)
        partition.foreach(record =>{
          val jSONObject: JSONObject = JSON.parseObject(record.value())
          val userInfoRediskey: String = "UserInfo" + jSONObject.getString("id")
          jedis.set(userInfoRediskey,record.value())
        })
        jedis.close()
      })
    })

    //开启连接
    ssc.start()
    ssc.awaitTermination()
  }
}
