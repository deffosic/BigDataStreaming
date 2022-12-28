package SparkBDD

import SessionSparkBDS.SessionBDS
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties
import org.elasticsearch.spark.sql._
import org.apache.commons.httpclient.HttpConnectionManager
import org.apache.commons.httpclient._

object Spark_and_ElasticSearch {

  def main(args: Array[String]): Unit ={
    val session_s = SessionBDS.Session_Spark(true)

    val prop = new Properties()
    prop.put("user","consultant")
    prop.put("password", "Makeda18!")

    val df_elasticSearch = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimeter", ";")
      .option("header", "true")
      .option("inferSchema","true")
      .load("orders_csv.csv")

    df_elasticSearch.show(10)

    df_elasticSearch.write
      .format("org.elasticsearch.spark.sql")
      .mode(SaveMode.Append)
      .option("es.port", "9288")²
      .option("es.node", "localhost")
      .option("es.net.http.auth.user", "elastic")
      .option("es.net.http.auth.pass", "password")
      .save("index_DSC/doc")

    //Ecriture Native avec ElasticSearch Spark

    val ss = SparkSession.builder()
      .appName("Mon APP Spark et ElasticSearch")
      .config("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.crossJoin.enable", "true")
      .config("es.port", "9288")
      .config("es.node", "localhost")
      .config("es.net.http.auth.user", "elastic")
      .config("es.net.http.auth.pass", "password")s
      .enableHiveSupport()

      df_elasticSearch.saveToEs("index_DSC/doc")



  }

}
