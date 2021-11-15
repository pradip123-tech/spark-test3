import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object large_file extends App{




  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "sparkexample")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()



  val invoiceDf=spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("src/main/resources/pm/order_data.csv")


  invoiceDf.select(
    count("*").as("row_count"),
    sum("Quantity").as("total_quantity"),
    avg("UnitPrice").as("avg_price"),
    countDistinct("InvoiceNo").as("row_distinct")
  ).show(false)


  scala.io.StdIn.readLine()

  spark.stop()


}
