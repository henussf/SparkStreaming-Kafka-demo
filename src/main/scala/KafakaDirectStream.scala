
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.logging.LogFactory
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}
import utils.{Param, ZkConnectionPool, ZkCustomSerializer}

import scala.util.{Failure, Success, Try}

/*
createDirectStream 方式接收数据
 */

object KafakaDirectStream {

  val logger = LogFactory.getLog(KafakaDirectStream.getClass)

  def main(args: Array[String]): Unit = {
    System.out.println("runJob")
    val ssc:StreamingContext = createSparkStreamingContext()

    System.out.println("runJob 1")

    val kafkaStream = createKafakaDirectStreamWithZk(ssc)
    kafkaStream.foreachRDD(rdd => {
      // 只处理有数据的rdd，没有数据的直接跳过
      if(!rdd.isEmpty()){

        println("rdd begin " + rdd.count())
        // 迭代分区，里面的代码是运行在executor上面
        rdd.foreachPartition(partitions => {

          println("partitions begin " )
          //如果没有使用广播变量，连接资源就在这个地方初始化
          //比如数据库连接，hbase，elasticsearch，solr，等等
          partitions.foreach(msg => {
            //            println("msg " + msg)
            println("msg key " + msg._1 + " , value "+ msg._2)
          })
        })
      }

    })


    processKafkaStreamWithZk(kafkaStream)

    System.out.println("runJob 2")
    ssc.start()
    ssc.awaitTermination()
  }

  //zhuxian
  private def createSparkStreamingContext(): StreamingContext = {
    val sc = new SparkConf().setAppName(Param.JOB_NAME)
          .setMaster("local[2]")
    //add gracefully stop
    sc.set("spark.streaming.stopGracefullyOnShutdown", "true")
    //Setting the max receiving rate
    sc.set("spark.streaming.backpressure.enabled", "true")
    val ssc = new StreamingContext(sc, Seconds(10))
    ssc
  }

  def createKafakaDirectStreamWithZk(ssc: StreamingContext): InputDStream[(String, String)] = {
    val topics = Set[String](Param.TOPIC_INPUT)
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> Param.ZK_CONNECT,
      "group.id" -> Param.GROUP,
      "bootstrap.servers" -> Param.BROKERS
    )
    val topic = topics.last
    val storedOffsets = getFromOffsetsFromZk(topic)
    val kafkaStream = storedOffsets match {
      case None =>
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      case Some(fromOffsets) =>
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    kafkaStream
  }

  def getFromOffsetsFromZk(topic:String): Option[Map[TopicAndPartition, Long]] = {
    val zkHosts = Param.ZK_HOSTS
    val zkClient = new ZkClient(zkHosts, 30000, 30000, new ZkCustomSerializer())
    val zkPathOffset:String = "/kafka/consumers/"+Param.GROUP+"/offsets/"+Param.TOPIC_INPUT
    logger.info("Reading offsets from Zookeeper")
    val stopwatch = new Stopwatch()
    val numChild = zkClient.countChildren(zkPathOffset)
    numChild match {
      case 0 =>
        logger info(s"No child found in $zkPathOffset" + stopwatch)
        None
      case _ =>
        val zkChildren = zkClient.getChildren(zkPathOffset)
        logger.info(s"zkChildren is : $zkChildren")
        val zkChildrenArr = zkChildren.toArray
        logger.info(s"zkChildrenArr is : $zkChildrenArr")
        val offsets = zkChildrenArr.map(child => TopicAndPartition(topic, child.toString.toInt) -> zkClient.readData(zkPathOffset+"/"+child.toString).toString.toLong).toMap
        logger.info(s"readOffsets offsets are: $offsets")
        Some(offsets)
    }
  }

  def processKafkaStreamWithZk(kafkaStream:InputDStream[(String, String)]) = {
    kafkaStream.foreachRDD{ rdd =>
      // executed at the driver
      logger.info("1. execute in kafkaStream.foreachRDD")
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      printOffsets(offsetRanges)

      rdd.foreachPartition { iterator =>
        // executed at the worker
        val offset: OffsetRange = offsetRanges(TaskContext.get().partitionId())
        printOffsets(Array(offset))
        //map and filter
        val partitionIterator = transformData(iterator)
        //save data and offset
        saveData(partitionIterator, ()=>saveKafkaOffsetsToZk(offset))
      }
    }
  }

  protected def transformData(iterator: Iterator[(String, String)]): Iterator[String] = {
    //don't call iterator.size() otherwise the data will lose
    logger.info(s"2. execute in rdd.foreachPartition. is iterator empty?  ${iterator.isEmpty}")
    iterator.map(_._2).map { record =>
      logger.info("2.1 execute in iterator.map")
      println(record)
      record
//      Try(JSON.parseObject(record, classOf[String])) match {
//        case Success(creditMessage: CreditMessage) => creditMessage
//        case Failure(e: Throwable) =>
//          logger.error(s"error when parse json: $record", e)
//          null
//      }
    }.filter { obj =>
      logger.info("2.2 execute in iterator.filter")
      Try(obj != null && !obj.equalsIgnoreCase("")) match {
        case Success(_) => true
        case _ =>
          println("filter error")
          false
      }
    }
  }

  protected def saveData(partitionIterator:Iterator[String], saveKafkaOffsets:()=>Unit): Unit = {
    //save message to hbase and kafka
    logger.info(s"2.3  execute in saveData(). is partitionIterator empty ?: ${partitionIterator.isEmpty}")
    if(partitionIterator.isEmpty){
      logger.info("2.4 execute in saveData(). partitionIterator is empty. return")
      return
    }

    logger.info("2.4 execute in get connection from hbase connection pool and kafka producer connection pool ")
    try {
      saveKafkaOffsets()
    } catch {
      case e: Exception =>
        logger.info("2.7 execute in process Exception")
        logger.error("error when send message", e)
    } finally {
      logger.info("2.8 execute in begin to execute finally")
    }
  }

  protected def saveKafkaOffsetsToZk(offset : OffsetRange): Unit = {
    logger.info(s"2.5 execute in saveKafkaOffsetsToZk(). before update zookeeper ")
    println(s"2.6 execute in saveKafkaOffsetsToZk(). before update zookeeper")
    val zkPathOffset:String = "/kafka/consumers/"+Param.GROUP+"/offsets/"+Param.TOPIC_INPUT+"/"+offset.partition.toString



    var ZkConnect:ZkClient = null
    val fromOffsetStr = offset.fromOffset.toString

    println(s"2.6 execute in saveKafkaOffsetsToZk().zkPathOffset " + zkPathOffset + " fromOffsetStr " + fromOffsetStr)
    try {
      ZkConnect = ZkConnectionPool.getZkConnection()
      ZkUtils.updatePersistentPath(ZkConnect, zkPathOffset, fromOffsetStr)
      logger.info(s"2.6 execute in saveKafkaOffsetsToZk(). after update zookeeper")
      println(s"2.6 execute in saveKafkaOffsetsToZk(). after update zookeeper")
        }catch {
      case e: Exception =>
        logger.info("data save to zookeeper failed")
    }finally {
      ZkConnectionPool.returnZkConnection(ZkConnect)
    }
  }



  private def printOffsets(offsetRanges: Array[OffsetRange]): Unit = {
    for (o <- offsetRanges) {
      println(s"time:${System.currentTimeMillis()} topic:${o.topic} partition:${o.partition} fromOffset:${o.fromOffset} untilOffset:${o.untilOffset}")
    }
  }

  class Stopwatch {
    private val start = System.currentTimeMillis()
    override def toString() = (System.currentTimeMillis() - start) + "ms"
  }


}