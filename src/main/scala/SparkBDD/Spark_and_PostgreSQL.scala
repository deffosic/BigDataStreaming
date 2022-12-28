package SparkBDD

import SessionSparkBDS.SessionBDS

import java.util.Properties

object Spark_and_PostgreSQL {

  def main(args: Array[String]): Unit ={
    val session_s = SessionBDS.Session_Spark(true)

    val prop = new Properties()
    prop.put("user","consult")
    prop.put("password", "Makeda18!")

    val df_postgresql = session_s.read.jdbc("jdbc:postgresql://172.20.192.144:5432/jea_db", "orders", prop)

    df_postgresql.show(15)

    df_postgresql.printSchema()

    val df_postgresql2 = session_s.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://172.20.192.144:5432/jea_db")
      .option("user","consult")
      .option("password", "Makeda18!")
      .option("dbtable","(SELECT state, city, SUM(round(numunits * totalprice)) AS commandes_totales FROM orders GROUP BY state, city) table_summiry")
      .load()

    df_postgresql2.show()

  }
}
