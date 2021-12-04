import org.apache.log4j.Level._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count

object joinPractice extends App {

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "new1")
  sparkConf.set("spark.master", "local[1]")

  Logger.getLogger("org").setLevel(ERROR)

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val CutomerDf= spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/pm/customer.csv")

 //CutomerDf.show()



  val OrderDf = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/orders.csv")


 val OrderNew= OrderDf.withColumnRenamed("CustomerID", "CustID")

  val JoinCondition = CutomerDf.col("CustomerID") === OrderNew.col("CustID")

val JoinDf= CutomerDf.join(OrderNew, JoinCondition, "left")
  .select("InvoiceNo", "Description", "Country", "CustomerID")

  val co =JoinDf.select(count("*")).distinct()

co.count()


}
