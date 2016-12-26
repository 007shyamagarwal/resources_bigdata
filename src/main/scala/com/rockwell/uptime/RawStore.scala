package com.rockwell.uptime

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import kafka.serializer.StringDecoder
import java.util.Properties
import java.io.FileInputStream
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat

object RawStore {
    val properties = new Properties()
    def main(args: Array[String]): Unit = {
        if (args.length < 1) {
            println("""No properties file given""")
            System.exit(1)
        }
        properties.load(new FileInputStream(args(0)))
        try{
        val ssc = StreamingContext.getOrCreate(properties.getProperty("checkpointDir"), createStream);
        ssc.start()
        ssc.awaitTermination()
        }catch {
          case e: Exception => e.printStackTrace()
        }
        
    }

  def createStream(): StreamingContext = {
    val debug = properties.getProperty("debug").toBoolean;
    val topic = Set(properties.getProperty("inletKafkaTopic"));
    val kafkaParams = Map[String, String]("metadata.broker.list" -> properties.getProperty("metadataBrokerList"), "broker.id" -> properties.getProperty("brokerId"), "auto.offset.reset" -> properties.getProperty("autoOffsetReset"))
    val conf = new SparkConf().setAppName(properties.getProperty("appName")).setMaster(properties.getProperty("master")) /*.set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", properties.getProperty("eventLogDir"))*/ .set("spark.ui.port", "4044")
    val ssc = new StreamingContext(conf, Seconds(properties.getProperty("batchDurationInSeconds").toInt))
    ssc.sparkContext.hadoopConfiguration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    ssc.sparkContext.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    try {
      val dataStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic);
      val mappedDataStream = dataStream.map(_._2);
      var sdf = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss a z");
      val uptime = mappedDataStream.foreachRDD { rdd =>
        if (!rdd.isEmpty()) {
          val rdd1 = rdd.filter(x => !Option(x).getOrElse("").isEmpty())
          val sqlContext = SQLContext.getOrCreate(rdd1.context)
          if(debug) println("Records received successfully ==========>  " + rdd1.count + "  at " + sdf.format(System.currentTimeMillis()))
          val dataFrame = sqlContext.jsonRDD(rdd)
          dataFrame.write.format("parquet").save(properties.getProperty("hdfsParquetPath") + System.currentTimeMillis()) 
          println("Saved data successfully in hdfs in parquet format ==========>  at " + sdf.format(System.currentTimeMillis()))
        }
      }
      ssc.checkpoint(properties.getProperty("checkpointDir"));
    } catch {
      case e: Exception => println(e.printStackTrace()) 
    }
    return ssc;
  }
}



