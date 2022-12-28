package streaming.kafka

import java.util.Properties
//import java.util.logging.{LogManager, Logger}
import org.apache.log4j.LogManager
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerBDS {

  private var trace_kafka  = LogManager.getLogger("Log_Console")

  /**
   *
   * @param kafkaBootStrapServers
   * @return
   */
  def getKafkaProducerParams (kafkaBootStrapServers : String) : Properties = {
    val props : Properties = new Properties()

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("acks", "all")
    props.put("boostrap.server", kafkaBootStrapServers)
    props.put("security.protocol", "SASL_PLAINTEXT")

    return props
  }

  /**
   *
   * @param kafkaBootStrapServers
   * @param topic_name
   * @param message
   * @return
   */

  def getKafkaProducer( kafkaBootStrapServers : String, topic_name : String, message : String) : KafkaProducer[String, String] = {
    trace_kafka.info(s"instanciation d'une instance du producer Kafka aux serveurs ${kafkaBootStrapServers}")
    lazy val producer = new KafkaProducer[String, String](getKafkaProducerParams(kafkaBootStrapServers))

    trace_kafka.info(s"message à publier dans le topic ${topic_name}, ${message}")
    val recordProducer = new ProducerRecord[String,String](topic_name, message)

    try{
      trace_kafka.info("publication du message encours ...")
      producer.send(recordProducer)
      trace_kafka.info("message publié avec succés !:)")
    } catch {
      case ex : Exception =>
        trace_kafka.error(s"erreur dans la publication du message ${message} dans le Log du topic ${topic_name} dans kafka : ${ex.printStackTrace()}")
        trace_kafka.info(s"la liste des paramètres pour la connexion du producer Kafka sont : ${getKafkaProducerParams(kafkaBootStrapServers)}")
    } finally {
      producer.close()
    }

    return  producer
  }


}
