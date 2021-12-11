import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object session_23 extends  App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  case  class Logging(level:String,datetime:String)

  def mapper(line:String): Logging={

    val fields=line.split(',')

    val logging:Logging=Logging(fields(0),fields(1))

    return  logging
  }




val spark=SparkSession
  .builder()
  .appName("sparkSql")
  .master("local[2]")
  .getOrCreate()

  import spark.implicits._


  val myList=List("WARN,216-05-15 04:55:14",
    "FATAL,216-05-15 04:55:14",
    "WARN,216-05-15 04:55:14",
    "INFO,216-05-15 04:55:14")


  val rdd1=spark.sparkContext.parallelize(myList)

  val rdd2=rdd1.map(mapper)


  val df1=rdd2.toDF()


  /** we want to use spark sql here */

  df1.createOrReplaceTempView("final")

  spark.sql("select level, collect_list(datetime) as datetime from final group by level order by level").show(false)







}
