import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sparksql extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","sparksqlex")
  sparkConf.set("spark.master", "local[2]")



  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()


   val orderDf = spark.read
     .format("csv")
     .option("inferSchema", "true")
     .option("header", "true")
     .load("src/main/resources/pm/adhar.csv")

//orderDf.show(false)
  orderDf.createOrReplaceTempView("Adhar")





 val sqlDf = spark.sql("select * from Adhar")


  sqlDf.show(false)

  val NewDF = orderDf.select()


  NewDF.show()



 val df1 = orderDf.selectExpr("Registrar","concat(District, '_Jilah') as NEW").show(false)














}
