package streaming.kafka

import org.apache.spark.streaming.Seconds

import java.util.Properties
import java.util.Collections
//import java.util.logging.{LogManager, Logger}
import java.time.Duration

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.log4j.LogManager

//import org.apache.arrow.vector.types.pojo.ArrowType.Duration
import org.apache.kafka.clients.consumer.{CommitFailedException, ConsumerRecord, KafkaConsumer}
import scala.collection.JavaConverters._

object KafkaConsumerBDS {

  private val trace_kafka = LogManager.getLogger("Log_Console")
  /**
   *
   * @param kafkaBootStrapServers
   * @param kafkaConsumerGroupID
   * @return
   */
  def getKafkaComsumerParams (kafkaBootStrapServers : String, kafkaConsumerGroupID : String) : Properties = {

    val props : Properties = new Properties()

    props.put("bootstrap.servers", kafkaBootStrapServers)
    props.put("auto.offset.reset", "lastest")
    props.put("groupe.id", kafkaConsumerGroupID)
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    return props

  }

  /**
   *
   * @param kafkaBootStrapServers
   * @param kafkaConsumerGroupID
   * @param topic_list
   * @return
   */
  def getKafkaConsumer(kafkaBootStrapServers : String, kafkaConsumerGroupID : String, topic_list : String) : KafkaConsumer[String, String] = {
    val consumer : KafkaConsumer[String, String] = new KafkaConsumer(getKafkaComsumerParams(kafkaBootStrapServers, kafkaConsumerGroupID))

    try{
      consumer.subscribe(Collections.singletonList(topic_list))

      while(true){
        val messages : ConsumerRecords[String, String] = consumer.poll(30)
        if(! messages.isEmpty){
          trace_kafka.info("Nombre de messages collectés dans la fénêtre:" +messages.count())
          for (record <- messages.asScala){
            println("Topic : " +record.topic()+
              ", key : "+record.key()+
              ", Value : "+record.value()+
              ",Offset :"+record.offset()+
              ", Partition : "+record.partition())

          }

          // Deuxième méthode de récupération des données dans le Log Kafka
          /*val recordIterator = messages.iterator()
          while(recordIterator.hasNext == true){
            val record = recordIterator.next()
            println("Topic : " +record.topic()+
              ", key : "+record.key()+
              ", Value : "+record.value()+
              ",Offset :"+record.offset()+
              ", Partition : "+record.partition())
          }*/
          try{
            consumer.commitAsync()
          } catch {
            case ex : CommitFailedException =>
              trace_kafka.error(s"erreur dans le commit des offset, kafka n'a pas reçu le jeton de reconnaissance confirmant que nous avons reçu les données ${ex.printStackTrace()}")

          }

        }

      }

    } catch {
      case excpt : Exception =>
        trace_kafka.error(s"erreur dans le consumer ${excpt.printStackTrace()}")

    }finally {
      consumer.close()
    }

    return consumer
  }

}
