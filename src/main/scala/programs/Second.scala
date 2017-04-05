package programs

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime


object Second {

  case class Booking(buId: Long, destination: String)
  case class PriceLog(logId: Long, buId: Long, price: BigDecimal, dateTime: DateTime)
  case class MergedLog(buId: Long, logId: Long, price: BigDecimal, dateTime: DateTime, destination: String)

  def mergeLogs(bookings: RDD[Booking], priceLogs: RDD[PriceLog]) = {

    val modifiedPrice: RDD[(Long, (Long, BigDecimal, DateTime))] = priceLogs.map(x => (x.buId, (x.logId, x.price, x.dateTime)))
      .reduceByKey {
        case (firstRecord, secondRecord) => {
          if (firstRecord._3.isAfter(secondRecord._3)) {
            firstRecord
          } else {
            secondRecord
          }
        }
      }

    val modifiedBookings = bookings.map { x => (x.buId, x.destination) }
    val joinedData = modifiedPrice.join(modifiedBookings)

    joinedData.foreach { println }

    joinedData

  }

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local", "HC")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val price = sc.parallelize(Seq(
      PriceLog(1, 1, 10, new DateTime(2016, 12, 12, 0, 0)),
      PriceLog(2, 2, 20, new DateTime(2016, 12, 13, 0, 0)),
      PriceLog(3, 3, 30, new DateTime(2016, 12, 19, 0, 0)),
      PriceLog(4, 1, 10, new DateTime(2016, 12, 15, 0, 0)),
      PriceLog(5, 2, 20, new DateTime(2016, 12, 16, 0, 0)),
      PriceLog(6, 3, 30, new DateTime(2016, 12, 17, 0, 0))))

    val bookings = sc.parallelize(Seq(
      Booking(1, "Berlin"),
      Booking(2, "Milan"),
      Booking(3, "Munich")))

    mergeLogs(bookings, price)

  }

}