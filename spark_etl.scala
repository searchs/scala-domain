Logger.getLogger("org".setLevel(Level.OFF))

val spark = SparkSession
    .builder
    .appName("tempro ETL Pipeline")
    .master("local[*]")
    .getOrCreate()


    
    //  Read data from CSV file and create a TempView
def strToDouble(str: String): Double = {
    try {
        str.toDouble
    } catch {
        case n: NumberFormatException => {
            println("Cannot cast " + str)
            -1
        }

    }
}



case class Payment(physician_id: String
                    , date_payment: String
                    , record_id: String
                    , payer: String
                    , amount: Double
                    , physician_speciality: String
                    , nature_of_payment: String )


val ds: Dataset[Payment] = spark.sql(
    """select physician_profile_id
    Date_of_Payment
    """.stripMargin).as[Payment]

    ds.cache()import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{desc, sum, udf}


object ETL extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder.appName("ETL pipeline").master("local[*]").getOrCreate()
  import spark.implicits._

  /*
     I - Read data from the csv file and create a temp view
  */

  def strToDouble(str: String): Double = {
    try{
      str.toDouble
    } catch {
      case n: NumberFormatException => {
        println("Cant cast " + str)
        -1
      }
      }
    }
  val toDouble = udf[Double, String](strToDouble(_))
  val df = spark.read.option("header","true").csv("/home/devola/Desktop/labo/SparkLab/datasets/open_payments_csv/OP_DTL_GNRL_PGYR2018_P06282019.csv")
  val df2 = df.withColumn("amount",toDouble(df("Total_Amount_of_Payment_USDollars")))
  df2.createOrReplaceTempView("payments")


/*
    II - Transform into a dataset of payment object
 */

/*
This class represent one row of our dataset, note that we selected only the fields that interest us
 */

  case class Payment(physician_id: String, date_payment: String, record_id: String, payer: String, amount: Double, physician_specialty: String, nature_of_payment: String)

  /*
  Now we select fields that interest us from the temp view
   */
  val ds: Dataset[Payment] = spark.sql(
    """select physician_profile_id as physician_id,
      | Date_of_payment as date_payment,
      | record_id as record_id,
      | Applicable_manufacturer_or_applicable_gpo_making_payment_name as payer,
      | amount,
      | physician_specialty,
      | nature_of_payment_or_transfer_of_value as nature_of_payment
      | from payments
      | where physician_profile_id is not null""".stripMargin).as[Payment]
  ds.cache()

/*
     III - Explore and query the Open Payment data with Spark Dataset
 */

  //print first 5 payment
  ds.show(5)

  //print the schema of ds
  ds.printSchema()

  // What are the Nature of Payments with reimbursement amounts greater than $1000 ordered by count?
  ds.filter($"amount" > 1000).groupBy("nature_of_payment")
    .count().orderBy(desc("count")).show(5)

  // what are the top 5 nature of payments by count?
  ds.groupBy("nature_of_payment").count().
    orderBy(desc("count")).show(5)

  // what are the top 10 nature of payment by total amount ?
  ds.groupBy("nature_of_payment").agg(sum("amount").alias("total"))
    .orderBy(desc("total")).show(10)

  // What are the top 5 physician specialties by total amount ?
  ds.groupBy("physician_specialty").agg(sum("amount").alias("total"))
    .orderBy(desc("total")).show(5,false)

  /*
      IV - Saving JSON Document
   */

  // Transform the dataset to an RDD of JSON documents :
  ds.write.mode("overwrite").json("/home/devola/Desktop/labo/SparkLab/datasets/open_payments_csv/OP_DTL_GNRL_PGYR2018_P06282019_demo.json")

  /*
      V - Saving data to database
   */
  val url = "jdbc:postgresql://localhost:5432/postgres"
  import java.util.Properties
  val connectionProperties = new Properties()
  connectionProperties.setProperty("Driver", "org.postgresql.Driver")
  connectionProperties.setProperty("user", "spark")
  connectionProperties.setProperty("password","spark")

  val connection = DriverManager.getConnection(url, connectionProperties)
  println(connection.isClosed)
  connection.close()

  val dbDataFrame = spark.read.jdbc(url,"payment",connectionProperties)
  dbDataFrame.show()

  ds.write.mode("append").jdbc(url,"payment",connectionProperties)
  dbDataFrame.show()

}






