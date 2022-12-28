package SparkBDD
import SessionSparkBDS.SessionBDS
object Spark_and_Hive {

  def main(args: Array[String]): Unit ={
    val session_s = SessionBDS.Session_Spark(true)
    //session_s.sql()
  }

}
