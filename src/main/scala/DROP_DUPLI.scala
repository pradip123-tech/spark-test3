import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format}
object DROP_DUPLI extends App{


  //Logger.getLogger("org").setLevel(Level.ERROR)

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




  val df3= df1.selectExpr("cast(orderdate as date) orderdate")


 val df4 = df3.select(col("orderdate"),date_format(col("orderdate"),"yyyy-MM").as("new_col"))

df4.show()
  df3.printSchema()
df3.coalesce(3)

 /** val df2 = df1
    .withColumn("orderdate", unix_timestamp(col("orderdate")
    .cast(DateType)))
    .withColumn("newid", monotonically_increasing_id())
    .dropDuplicates("orderdate","customerid")
    .drop("orderid").count() **/


  scala.io.StdIn.readLine()
  case class Stuff(a:String,b:Int)

  val sc= spark.sparkContext




  val d= sc.parallelize(Seq( ("a",1),("b",2),
    ("",3) ,("d",4)).map { x => Stuff(x._1,x._2)  })

  val sum=0
  val sumData=Array(1,2,3,4)

  val rdd=sc.parallelize(sumData)

  rdd.foreach(x => x+sum)

}













