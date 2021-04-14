import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
object pm1 extends  App {
   //Logger.getLogger("org").setLevel(Level.ERROR)



  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "sparkexample")
  sparkConf.set("spark.master", "local[2]")


  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  case class OrderData (symbol: StringType, date: StringType, price: DoubleType)

  //to define schema explicitly

  //val NewSchema = StructType(StructField(

  //))

val NewSchema = StructType(Array(
  StructField("symbol", StringType),
  StructField("date", StringType),
  StructField("price", DoubleType)
))


  val df = spark.read
    .schema(NewSchema)
    .option("header","true")
    .option("mode","FailFast")
    .csv("src/main/resources/pm.csv")

  //val ds = df.as[OrderData]



  df.show(false)
  df.printSchema()


  df.withColumn("newprice", expr("price > 40")).show(false)

df.select(col("price"),col("symbol"))


  spark.sql("create database if not exists retail")

df.write
  .format("csv")
  .mode(SaveMode.Overwrite)
  .partitionBy("symbol")
  .saveAsTable("retail.orderss")


  spark.catalog.listTables("retail").show()





  //Logger.getLogger(getClass.getName).info("my application is completed successfully")

 scala.io.StdIn.readLine()

}
