package programs

import org.apache.spark.rdd.RDD
import org.joda.time._
import org.apache.spark.SparkContext


object Third {

  case class SentMailLog(userId: String, mailingId: Long, created: DateTime)
  case class OpenMailLog(userId: String, mailingId: Long, created: DateTime)
  case class MailClickLog(userId: String, mailingId: Long, created: DateTime)

  def calculateStatistics(sentMails: RDD[SentMailLog], openMail: RDD[OpenMailLog], clickMail: RDD[MailClickLog]) = {

    // Open Rate
    val sentMailsKeyed = sentMails.map { element =>
      ((element.userId, element.mailingId), element.created)
    }

    val openMailKeyed = openMail.map { element =>
      ((element.userId, element.mailingId), element.created)
    }

    val coGroupedData = sentMailsKeyed.cogroup(openMailKeyed)

    val openRatePerUidCid = coGroupedData.map {
      case (k, v) =>
        (k, getRate(v._1.toSeq, v._2.toSeq))
    }

    openRatePerUidCid.foreach { println }

    // Click Rate

    val clickMailKeyed = clickMail.map { element =>
      ((element.userId, element.mailingId), element.created)
    }

    val coGroupedData2 = sentMailsKeyed.cogroup(clickMailKeyed)

    val clickRatePerUidCid = coGroupedData2.map {
      case (k, v) =>
        (k, getRate(v._1.toSeq, v._2.toSeq))
    }

    clickRatePerUidCid.foreach { println }

  }

  def getRate(a: Seq[DateTime], b: Seq[DateTime]) = {

    var intervalSeq: Seq[Interval] = Seq.empty

    for (i <- 0 until a.length) {
      if (i == a.length - 1) {
        intervalSeq = intervalSeq ++ Seq(new Interval(a(i), new DateTime(Long.MaxValue)))
      } else {
        intervalSeq = intervalSeq ++ Seq(new Interval(a(i), a(i + 1)))
      }
    }

    var finalSeq: Seq[Interval] = Seq.empty

    b.foreach { openDate =>
      intervalSeq.foreach { interval =>
        if (openDate.isEqual(interval.getStart) || (openDate.isAfter(interval.getStart) && openDate.isBefore(interval.getEnd))) {
          finalSeq = finalSeq ++ Seq(interval)
        }
      }
    }

    (finalSeq.distinct.length.toDouble / a.length.toDouble)

  }

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local", "HC")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val sentMails = sc.parallelize(Seq(
      SentMailLog("12", 1, new DateTime(2016, 12, 1, 0, 0)),
      SentMailLog("12", 1, new DateTime(2016, 12, 4, 0, 0)),
      SentMailLog("12", 1, new DateTime(2016, 12, 8, 0, 0)),
      SentMailLog("12", 1, new DateTime(2016, 12, 12, 0, 0)),
      SentMailLog("12", 1, new DateTime(2016, 12, 16, 0, 0)),
      SentMailLog("13", 1, new DateTime(2016, 12, 1, 0, 0)),
      SentMailLog("13", 1, new DateTime(2016, 12, 5, 0, 0)),
      SentMailLog("13", 1, new DateTime(2016, 12, 15, 0, 0)),
      SentMailLog("13", 1, new DateTime(2016, 12, 20, 0, 0)),
      SentMailLog("13", 1, new DateTime(2016, 12, 25, 0, 0)),
      SentMailLog("13", 1, new DateTime(2016, 12, 29, 0, 0))))

    val openMail = sc.parallelize(Seq(
      OpenMailLog("12", 1, new DateTime(2016, 12, 2, 0, 0)),
      OpenMailLog("12", 1, new DateTime(2016, 12, 5, 0, 0)),
      OpenMailLog("12", 1, new DateTime(2016, 12, 6, 0, 0)),
      OpenMailLog("12", 1, new DateTime(2016, 12, 21, 0, 0)),
      OpenMailLog("12", 1, new DateTime(2016, 12, 21, 0, 0)),
      OpenMailLog("13", 1, new DateTime(2016, 12, 1, 0, 0)),
      OpenMailLog("13", 1, new DateTime(2016, 12, 1, 0, 0)),
      OpenMailLog("13", 1, new DateTime(2016, 12, 1, 0, 0)),
      OpenMailLog("13", 1, new DateTime(2016, 12, 1, 0, 0)),
      OpenMailLog("13", 1, new DateTime(2016, 12, 1, 0, 0))))

    val clickMail = sc.parallelize(Seq(
      MailClickLog("12", 1, new DateTime(2016, 12, 2, 0, 0)),
      MailClickLog("12", 1, new DateTime(2016, 12, 5, 0, 0)),
      MailClickLog("12", 1, new DateTime(2016, 12, 6, 0, 0)),
      MailClickLog("13", 1, new DateTime(2016, 12, 2, 0, 0)),
      MailClickLog("13", 1, new DateTime(2016, 12, 5, 0, 0)),
      MailClickLog("13", 1, new DateTime(2016, 12, 15, 0, 0)),
      MailClickLog("13", 1, new DateTime(2016, 12, 20, 0, 0)),
      MailClickLog("13", 1, new DateTime(2016, 12, 25, 0, 0)),
      MailClickLog("13", 1, new DateTime(2016, 12, 26, 0, 0)),
      MailClickLog("13", 1, new DateTime(2016, 12, 29, 0, 0))))

    calculateStatistics(sentMails, openMail, clickMail)

  }

}