package com.atguigu.handle

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.{lang, util}

object DauHandle {

  /**
   * 批次内去重
   * @param filterByRedisDStream
   */
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    val midAndLogDateToLogDStream: DStream[(String, StartUpLog)] = filterByRedisDStream.map(log => {
      (log.mid + log.logDate, log)
    })
    val midAndLogDateToIterLogDStream: DStream[(String, Iterable[StartUpLog])] = midAndLogDateToLogDStream.groupByKey()
    val midAndLogDateToListLogDStream: DStream[(String, List[StartUpLog])] = midAndLogDateToIterLogDStream.mapValues(log => {
      log.toList.sortWith(_.ts < _.ts).take(1)
    })
    val value: DStream[StartUpLog] = midAndLogDateToListLogDStream.flatMap(_._2)

    value
  }


  /**
   * 批次间去重（首先获取redis中保存的mid -> 拿当前mid与查询出的mid做对比，如果有重复 则过滤掉）
   *
   * @param startUpLogDStream
   * @return
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog],sc:SparkContext) = {
  /*  val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
      //创建redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      //查询redis中的数据
      val redisKey: String = "DAU" + log.logDate
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //拿当前的mid与查询出来的mid做对比进行过滤
      val bool: Boolean = mids.contains(log.mid) //true留下 false过滤掉

      //关闭连接
      jedis.close()
      !bool
    })
    value
  */

    //优化：每一个分区创建一个redis连接 以减少连接个数
   /* val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val logs: Iterator[StartUpLog] = partition.filter(log => {
        val redisKey: String = "DAU" + log.logDate
        val bool: lang.Boolean = jedis.sismember(redisKey, log.mid)
        !bool
      })
      jedis.close()
      logs
    })
    value
    */

    //优化：每一个批次创建一个redis连接
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {

      //在Driver端获取连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val redisKey: String = "DAU" + sdf.format(System.currentTimeMillis())
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //将Driver端查询到的mids数据广播到Exec端
      val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)
      val filterRDD: RDD[StartUpLog] = rdd.filter(log => {
        val bool: Boolean = midsBC.value.contains(log.mid)
        !bool
      })

      jedis.close()
      filterRDD
    })
    value



  }

  /*
  存什么：mid
  用什么类型：Set
  redisKey设计： "DAU:"logDate

  redis的set能去重为什么还要分组内去重 因为redis只存储mid 我们需要把明细数据存储到HBase中
 */


  /**
   * 将mid保存至redis
   *
   * @param startUpLogDStream
   */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {

    //foreachRDD是SparkStreaming的输出原语  foreach、foreachPartition是SparkCore的算子
    //当涉及到写库操作时要用foreachRDD
    startUpLogDStream.foreachRDD(rdd => {
      //如果在这里创建连接是在Driver端执行 不能到达Exec端
      rdd.foreachPartition(partition => {
        //创建Redis连接  （Exec端）
        val jedis: Jedis = new Jedis("hadoop102", 6379)
        partition.foreach(log => {
          val redisKey: String = "DAU" + log.logDate
          jedis.sadd(redisKey, log.mid)
        })
        //关闭连接
        jedis.close()
      })

    })

  }
}