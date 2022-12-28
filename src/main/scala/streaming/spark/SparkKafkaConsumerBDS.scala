package streaming.spark


import java.time.Duration
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.protocol
import org.apache.kafka.common.serialization._
import org.apache.kafka.common._
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.spark.streaming.kafka010.KafkaUtils._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.kafka.common.security.auth._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.dstream.InputDStream
import java.util.Properties
import java.util.Collections

import scala.collection.JavaConverters._
import org.apache.kafka.clients.producer._
import org.apache.spark.streaming.StreamingContext


//import java.util.Properties

//import java.util.logging.{LogManager, Logger}
import org.apache.log4j.LogManager
import SessionSparkBDS.SessionBDS.getSparkStreamingContext
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.security.auth.SecurityProtocol
//import org.apache.kafka.common.serialization.StringDeserializer


object SparkKafkaConsumerBDS {

  private var trace_kafka: Logger = LogManager.getLogger("Log_Console")

/*
  /**
   *
   * @param kafkaBootStrapServer
   * @param kafkaConsumerGroupId
   * @param kafkaConsumerReadOrder
   * @param kafkaZookeeper
   * @param kafkaKerberosName
   * @return
   */
  def  getSparkKafkaConsumerParams(kafkaBootStrapServer : String, kafkaConsumerGroupId : String, kafkaConsumerReadOrder : String,
                                   kafkaZookeeper : String, kafkaKerberosName : String) : Properties ={
    val props = new Properties()

    props.put("bootsrap.server", kafkaBootStrapServer)
    props.put("group.id",kafkaConsumerGroupId)
    props.put("zookeeper.hosts", kafkaZookeeper)
    props.put("auto.offset.reset", kafkaConsumerReadOrder)
    props.put("enable.auto.commit", false)
    props.put("key.deserializer", classOf[StringDeserializer])
    props.put("value.deserializer", classOf[StringDeserializer])
    props.put("sasl.kerberos.service.name", kafkaKerberosName)
    props.put("security.protocol", SecurityProtocol.PLAINTEXT)

    return  props
  }
*/

  def  getSparkKafkaConsumerParams(kafkaBootStrapServer : String, kafkaConsumerGroupId : String, kafkaConsumerReadOrder : String,
                                   kafkaZookeeper : String, kafkaKerberosName : String) : Map[String, Object] = {
    var kafkaParam : Map[String, Object] =  Map(
      "bootsrap.server" -> kafkaBootStrapServer,
      "group.id" -> kafkaConsumerGroupId,
      "zookeeper.hosts" -> kafkaZookeeper,
      "auto.offset.reset" -> kafkaConsumerReadOrder,
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "sasl.kerberos.service.name" -> kafkaKerberosName,
      "security.protocol" -> SecurityProtocol.PLAINTEXT
    )

    return  kafkaParam
  }

  def getKafkaConsummer (kafkaBootStrapServer : String, kafkaConsumerGroupId : String,
                         kafkaConsumerReadOrder : String, kafkaZookeeper : String,
                         kafkaKerberosName : String, batchDuration : Int,
                         kafkaTopics : Array[String] ) : InputDStream[ConsumerRecord[String, String]] = {
    var props : Map[String, Object] = Map(null, null)
    var consumerKafka : InputDStream[ConsumerRecord[String, String]] = null

    try{

      val ssc = getSparkStreamingContext(env= true, batchDuration)
      props = getSparkKafkaConsumerParams(kafkaBootStrapServer, kafkaConsumerGroupId, kafkaConsumerReadOrder, kafkaZookeeper, kafkaKerberosName)
      consumerKafka = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](kafkaTopics, props)
      )

    }catch{
      case ex : Exception =>
        trace_kafka.error(s"erreur dans l'initialisaton du consumer kafka ${ex.printStackTrace()}")
        trace_kafka.info(s"La liste des paramètres pour la connexion du consumer kafka sont : ${props}")
    }


    return  consumerKafka
  }

}
