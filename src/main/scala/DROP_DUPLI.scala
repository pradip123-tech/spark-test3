import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, unix_timestamp}
import org.apache.spark.sql.types.DateType

object DROP_DUPLI extends App{


  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","sparksqlex")
  sparkConf.set("spark.master", "local[2]")



  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()


  val myList = List(
    (1,"2013-07-25",11599,"CLOSED"),
    (2,"2014-07-25",256,"PENDING_PAYMENT"),
    (3,"2013-07-25",11599,"COMPLETE"),
    (4,"2019-07-25",8827,"CLOSED")
  )


val df1 = spark.createDataFrame(myList).toDF("orderid","orderdate","customerid","status")


  df1.show()


val df2 = df1
  .withColumn("orderdate", unix_timestamp(col("orderdate")
  .cast(DateType)))
  .withColumn("newid", monotonically_increasing_id())
  .dropDuplicates("orderdate","customerid")
  .drop("orderid").count()



}
