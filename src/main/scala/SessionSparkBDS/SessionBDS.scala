package SessionSparkBDS

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD

import java.io.FileNotFoundException


object SessionBDS {

  var ss : SparkSession = null

  var sparkConf : SparkConf = null

  val trace_log: Logger = LogManager.getLogger("Session_Spark")

  /**
   * fonction qui initialise et instancie une session spark
   * @param env : c'est une variable  qui indique l'environnement sur lequel notre application est déployée
   *              si Env = True, alors l'application est local, sinon, elle est déployée sur un cluster
   *
   */

  def Session_Spark (env : Boolean = true) : SparkSession = {
    try {
      if(env == true) {
        System.setProperty("hadoop.home.dir", "/home/cedric/Hadoop")
        ss = SparkSession.builder
          .master("local[*]")
          .config("spark.sql.crossJoin.enabled", "true")
          //.enableHiveSupport()
          .getOrCreate()
      }else{
        ss = SparkSession.builder()
          .appName(name="Mon application Spark")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.crossJoin.enabled", "true")
          .enableHiveSupport()
          .getOrCreate()
      }

    } catch {
      case ex : FileNotFoundException => trace_log.error(s"Nous n'avons pas trouvé le winutils (sous window) ou le chemin d' hadoop (sous linux) :${ex.printStackTrace()}")
      case ex : Exception =>trace_log.error(s"Erreur dans l'initialisation de la session Spark: ${ ex.printStackTrace()}")
    }
    return  ss
  }

  def main(args: Array[String]): Unit={
    try {
      val sc = Session_Spark(true).sparkContext

      val rdd_test : RDD[String] = sc.parallelize(List("Paul", "Xavier", "Anne", "Makeda", "Nyeleti"))

      rdd_test.foreach{
        l =>println(l)
      }
    } catch {
      case e: Exception => println(s"Error : ${e.printStackTrace()}")
    }

  }


  /**
   * fonction qui initialise le contexte Spark Streaming
   * @param env : environnement sur lequel est déployé notre application. Si true, alors on est en localhost
   * @param duree_batch : c'est le SparkStreamingBatchDuration - où la durée du micro-batch
   * @return : la fonction renvoie en résultat une instance du contexte Streaming
   */

  def getSparkStreamingContext (env : Boolean = true, duree_batch : Int) : StreamingContext = {
    trace_log.info("initialisation du contexte Spark Streaming")
    if (env) {
      sparkConf = new SparkConf().setMaster("local[*]")
        .setAppName("Mon application streaming")
    } else {
      sparkConf = new SparkConf().setAppName("Mon application streaming")
    }
    trace_log.info(s"la durée du micro-bacth Spark est définie à : $duree_batch secondes")
    val ssc : StreamingContext = new StreamingContext(sparkConf, Seconds(duree_batch))

    return ssc

  }



}
