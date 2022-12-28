package SparkBDD

import SessionSparkBDS.SessionBDS

import org.apache.spark.sql.execution.datasources._
import org.apache.hadoop.hbase._

import java.util.Properties

object Spark_and_Hbase {

  def catalog_orders : String =
    s"""{
      |"table":{"namespace":"default", "name":"table_orders"},
      |"rowkey":"key",
      |"columns":{
      |"order_id":{"cf":"rowkey", "col":"key", "type":"string"},
      |"custumer_id":{"cf":"orders", "col":"custumerid", "type":"string"},
      |"campaign_id":{"cf":"orders", "col":"campaignid", "type":"string"},
      |"order_date":{"cf":"orders", "col":"orderdate", "type":"string"},
      |"city":{"cf":"orders", "col":"city", "type":"string"},
      |"state":{"cf":"orders", "col":"state", "type":"string"},
      |}
      |}""".stripMargin

  def main(args: Array[String]): Unit ={
    val session_s = SessionBDS.Session_Spark(true)

    val prop = new Properties()
    prop.put("user","consultant")
    prop.put("password", "Makeda18!")

    val df_hbase = session_s.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog_orders))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    df_hbase.printSchema()
    df_hbase.show(false)

    df_hbase.createOrReplaceTempView("Orders")
    session_s.sql("select * from Orders where state= 'MA'").show()

  }

}
