package ttorbjornsen

import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.HashMap

import kafka.producer.{KeyedMessage, Producer}
import org.jsoup.Jsoup
import org.jsoup.nodes._
import play.api.libs.json._

import scala.collection.JavaConversions._
import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}




/**
  * Created by torbjorn.torbjornsen on 04.07.2016.
  */

case class AcqCarHeader(finnkode:String, location:String, title:String, year:String, km:String, price:String)

object Utility {

  def getURL(url: String)(retry: Int): Try[Document] = Try(Jsoup.connect(url).
    userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.120 Safari/535.2").
    followRedirects(true).
    execute().parse())
    .recoverWith {
      case _ if(retry > 0) => {
        Thread.sleep(3000)
        println("Retry url " + url + " - " + retry + " retries left")
        getURL(url)(retry - 1)
      }
    }

  def saveFinnCarsPageResults(producer:Producer[String,String], topic:String, url: String):Unit = {
    //val pageResult = Future{Source.fromURL(new URL(url)).mkString}
    val pageResult = Future{scrapeCarHeadersFromPage(url).toString}

    val action = pageResult.map {results =>
      //producer.send(new KeyedMessage[String, String](topic, results))
      println("success - extracting url " + url + results)
    } recover{
      case t: Throwable => {
        t.printStackTrace()
//        println("failure - extracting url " + url)
        saveFinnCarsPageResults(producer, topic, url) //retry

      }
    }

    Await.result(action, 4 minutes)
  }



  def scrapeCarHeadersFromPage(url:String)= {
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=78647939"
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=77386827" //sold
    //val url = "http://m.finn.no/car/used/ad.html?finnkode=78601940" //deleted page
    //val url = "http://m.finn.no/car/used/search.html?year_from=2003&year_to=2003&body_type=4&page=3"
    val doc: Try[Document] = getURL(url)(10)

    val carHeadersFromPageList = new ListBuffer[JsObject]()

    doc match {
      case Success(doc) => {
        val rubrikkElements = doc.select("a[data-finnkode]")
        for (rubrikkElement <- rubrikkElements) {
          val finnkode = rubrikkElement.attr("data-finnkode")
          val location = rubrikkElement.getElementsByAttributeValue("data-automation-id", "topRowCenter").text()
          val title = rubrikkElement.getElementsByAttributeValue("data-automation-id", "titleRow").text()
          val bodyRowElement = rubrikkElement.getElementsByAttributeValue("data-automation-id", "bodyRow")
          val year = bodyRowElement.get(0).text
          val km = if (bodyRowElement.size() > 2) {
            bodyRowElement.get(1).text
            } else if (bodyRowElement.get(1).text.contains("km")) {
            bodyRowElement.get(1).text
          } else Utility.Constants.EmptyString

          val price = if (bodyRowElement.size() > 2) {
            bodyRowElement.get(2).text
          } else if (bodyRowElement.get(1).text.contains("km")) {
            Utility.Constants.EmptyString
          } else bodyRowElement.get(1).text


          val jsObj = Json.obj("finnkode" -> finnkode, "location" -> location, "title" -> title, "year" -> year, "km" -> km, "price" -> price)
          //DAO.writeCarHeader(jsObj)
          carHeadersFromPageList += jsObj
        }
      }

      case Failure(e) => {
        val jsObj = Json.obj("finnkode" -> "NULL", "location" -> "NULL", "title" -> "NULL", "year" -> "NULL", "km" -> "NULL", "price" -> "NULL")
        carHeadersFromPageList += jsObj
      }
    }
//    carHeadersFromPageList.toList
    JsArray(carHeadersFromPageList)
  }


  def printCurrentMethodName() : Unit = println(Thread.currentThread.getStackTrace()(2).getMethodName)

  object Constants {
    val EmptyMap = new java.util.HashMap[String,String](Map("NULL" -> "NULL"))
    val EmptyList = Set("NULL")
    val EmptyString = "NULL"
    val EmptyInt = -1
    val EmptyDate = "1900-01-01"
    val ETLSafetyMargin = 7 //days
    val ETLFirstLoadDate = LocalDate.of(2016,7,1)
  }

}
