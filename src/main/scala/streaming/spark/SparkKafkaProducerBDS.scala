package streaming.spark

import java.util.Properties
//import java.util.logging.{LogManager, Logger}
import org.apache.log4j.LogManager
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object SparkKafkaProducerBDS {

  private var trace_kafka = LogManager.getLogger("Log_Console")

  def getSparkKafkaProducerParams (kafkaBootStrapServers : String) : Properties = {
    val props : Properties = new Properties()

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("acks", "all")
    props.put("boostrap.server", kafkaBootStrapServers)
    props.put("security.protocol", "SASL_PLAINTEXT")

    return props
  }


  def getProducerKafka (kafkaBootStrapServers : String, topic_name : String, message : String) : KafkaProducer[String, String] = {

    trace_kafka.info("Instanciation d'une instance du producer Kafka aux serveurs" + kafkaBootStrapServers)
    lazy val producer_Kafka = new KafkaProducer[String, String]( getSparkKafkaProducerParams(kafkaBootStrapServers))

    trace_kafka.info(s"Message à publier dans le topic ${topic_name}, ${message}")
    val record_publish = new ProducerRecord[String,String](topic_name, message)

    try {

      trace_kafka.info("Publication du message")
      producer_Kafka.send(record_publish)

    } catch{
      case ex : Exception =>
        trace_kafka.error(s"erreur dans la publication du message dans kafka ${ex.printStackTrace()}")
        trace_kafka.info(s"La liste des paramètres pour la connexion du producer kafka sont : ${getSparkKafkaProducerParams(kafkaBootStrapServers)}")
    } finally {

      producer_Kafka.close()
    }

    return  producer_Kafka

  }


}
