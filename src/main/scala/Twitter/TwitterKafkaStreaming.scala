package Twitter

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import java.util.Collections
//import java.util.logging.{LogManager, Logger}
import org.apache.log4j.LogManager

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.{Client, Constants}
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.{Authentication, OAuth1}

import twitter4j._
import twitter4j.conf.{Configuration, ConfigurationBuilder}

import streaming.kafka.KafkaProducerBDS.getKafkaProducer

import scala.collection.JavaConverters._

class TwitterKafkaStreaming {

  private val trace_kafka  = LogManager.getLogger("Log_Console")


  /**
   * Cette méthode définie un client  Hosebird. Ce client permet de collecter les tweets  contenant une liste d'hashtags et de les publiers en temps réel dans un ou plusieurs
   * @param kafkaBootStrapServers
   * @param topic
   * @param Consumer_key
   * @param Consumer_secret
   * @param Access_token
   * @param Token_secret
   * @param liste_hashtags
   */
  def producerTwitterKafkaHbc(kafkaBootStrapServers : String, topic : String, Consumer_key : String,
                              Consumer_secret : String, Access_token : String, Token_secret : String, liste_hashtags : String) : Unit = {
    val queue : BlockingQueue[String] = new LinkedBlockingQueue[String](10000)

    val auth : Authentication = new OAuth1(
      Consumer_key,
      Consumer_secret,
      Access_token,
      Token_secret
    )

    val endp : StatusesFilterEndpoint = new StatusesFilterEndpoint()
    endp.trackTerms(Collections.singletonList(liste_hashtags))
    endp.trackTerms(List(liste_hashtags).asJava)


    val client_hbc_params : ClientBuilder = new ClientBuilder()
      .hosts(Constants.STREAM_HOST)
      .authentication(auth)
      .gzipEnabled(true)
      .endpoint(endp)
      .processor(new StringDelimitedProcessor(queue))

    val client_hbc : Client = client_hbc_params.build()
    try{

      client_hbc.connect()

      while(client_hbc.isDone){
        val message : String = queue.poll(15, TimeUnit.SECONDS)
        getKafkaProducer(kafkaBootStrapServers, topic, message)
        println(s"Message Twitter : $message")
      }

    } catch {
      case ex : InterruptedException => trace_kafka.error("Le client HBS a été interrompu"+ ex.printStackTrace())
    } finally {
      client_hbc.stop()
      getKafkaProducer(kafkaBootStrapServers, topic, "").close()
    }

  }

  def producerTwitter4JKafka(kafkaBootStrapServers : String, topic : String, Consumer_key : String,
                              Consumer_secret : String, Access_token : String, Token_secret : String, liste_hashtags : String) : Unit = {
    val queue : BlockingQueue[String] = new LinkedBlockingQueue[String](1000)
    val twitterConf : ConfigurationBuilder = new ConfigurationBuilder()

    twitterConf
      .setJSONStoreEnabled(true)
      .setDebugEnabled(true)
      .setOAuthConsumerKey(Consumer_key)
      .setOAuthConsumerSecret(Consumer_secret)
      .setOAuthAccessToken(Access_token)
      .setOAuthAccessTokenSecret(Token_secret)


    val twitterStream : TwitterStream = new TwitterStreamFactory(twitterConf.build()).getInstance()

    val listener : StatusListener= new StatusListener {
      override def onStatus(status: Status): Unit = {
        trace_kafka.info("Evènement d'ajout de tweets détectés : " + status.getText)
        queue.put(status.getText)
        getKafkaProducer(kafkaBootStrapServers, topic_name = topic, message = status.getText) // première méthode

      }

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {

      }

      override def onTrackLimitationNotice(i: Int): Unit = {


      }

      override def onScrubGeo(l: Long, l1: Long): Unit = {


      }

      override def onStallWarning(stallWarning: StallWarning): Unit = {


      }

      override def onException(e: Exception): Unit = {

        trace_kafka.error("Erreur généré par Twitter : " + e.printStackTrace())

      }
    }
    twitterStream.addListener(listener)
    twitterStream.sample()  // cette méthode déclanche la reception des données

    val filter: FilterQuery = new FilterQuery().track(liste_hashtags)
    twitterStream


  }

}
