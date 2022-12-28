package SparkBDD

import SessionSparkBDS.SessionBDS

import java.util.Properties

object Spark_and_SqlServer {

  def main(args: Array[String]): Unit ={
    val session_s = SessionBDS.Session_Spark(true)

    val prop = new Properties()
    prop.put("user","consultant")
    prop.put("password", "Makeda18!")

    val df_sqlserver= session_s.read.jdbc("jdbc:sqlserver://mssql/172.20.192.144:1433; databaseName=jea_db", "orders", prop)

    df_sqlserver.show(15)

    df_sqlserver.printSchema()

    val df_sqlserver2 = session_s.read
      .format("jdbc")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://mssql/172.20.192.144:1433; databaseName=jea_db; useNTMLV2=true")
      .option("user","consultant")
      .option("password", "Makeda18!")
      .option("dbtable","(SELECT state, city, SUM(round(numunits * totalprice)) AS commandes_totales FROM orders GROUP BY state, city) table_summiry")
      .load()

    df_sqlserver2.show()

  }

}
