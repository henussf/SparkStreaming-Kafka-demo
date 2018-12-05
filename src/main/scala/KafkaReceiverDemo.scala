import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.Param

/*
receiver 方式接收数
 */
object KafkaReceiverDemo {

  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    iter.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(i => (x, i)) }
  }

  def main(args: Array[String]) {
    LoggerLevels.setStreamingLogLevels()

//    val Array(zkQuorum, group, topics, numThreads) = args

    val zkQuorum = Param.ZK_CONNECT

    val topics = Param.TOPIC_INPUT

    val group = "clientid"

    val numThreads = "2"

    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("D:\\sparkStreaming")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val rdds = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
//    val words = data.map(_._2).flatMap(_.split(" "))
//    val wordCounts = words.map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
//    wordCounts.print()
    rdds.foreachRDD(rdd => {
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
            println("msg " + msg._2)
          })
        })
      }

    })

    ssc.start()
    ssc.awaitTermination()
  }

}
