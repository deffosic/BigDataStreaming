package SparkBDD

import SessionSparkBDS.SessionBDS

import java.util.Properties

object Spark_and_Mysql {

  def main(args: Array[String]): Unit ={
    val session_s = SessionBDS.Session_Spark(true)

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password", "Makeda18!")

    val df_mysql = session_s.read.jdbc("jdbc:mysql://172.20.192.144:3306/jea_db", "jea_db.orders", prop)

    df_mysql.show(15)

    df_mysql.printSchema()

    val df_mysql2 = session_s.read
      .format("jdbc")
      .option("url", "jdbc:mysql://172.20.192.144:3306/jea_db")
      .option("user","root")
      .option("password", "Makeda18!")
      //.option("query","selact state, city, sum(round(numunits * totalprice) as commandes_totales from jea_db.orders group by state, city")
      .option("dbtable","(SELECT state, city, SUM(round(numunits * totalprice)) AS commandes_totales FROM orders GROUP BY state, city ) table_summiry")
      .load()

    df_mysql2.show()

  }
}
