package ttorbjornsen


import java.time.LocalDateTime
import java.util.Properties

import kafka.producer.{Producer, ProducerConfig}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}


object ExtractFinnCars extends App {
  //Thread.sleep(20000) //sleep while spark docker container is getting ready. Should be moved to dockerfile.

  import java.sql.{Connection, DriverManager, ResultSet};

  // Change to Your Database Config
  val password = System.getProperty("password")
  val username = System.getProperty("username")
  val conn_str = "jdbc:postgresql://localhost:5432/finncars_db?user=" + username + "&password=" + password

  // Setup the connection
  val conn = DriverManager.getConnection(conn_str)

  System.setProperty("javax.net.ssl.trustStore", "c:\\temp\\finn-ssl-certificate.jks") //https://stackoverflow.com/questions/7744075/how-to-connect-via-https-using-jsoup

  // each Future will async try to extract page results from Kafka and
  val hdrPages1 = Range(1,100,1)
  val hdrPages2 = Range(100,200,1)

  val f1:Future[Unit] = Future{
    hdrPages1.map{page =>
      val url = "https://www.finn.no/car/used/search.html?year_from=2007&body_type=4&rows=100&page=" + page
      Utility.saveFinnCarsPageResults(url, conn)
      println("Page " + page + " written to postgres " + " . Time : " + LocalDateTime.now().toString)
      Thread.sleep(5000)
    }
  }

  val f2:Future[Unit] = Future{
    hdrPages2.map{page =>
      val url = "https://www.finn.no/car/used/search.html?year_from=2007&body_type=4&rows=100&page=" + page
      Utility.saveFinnCarsPageResults(url, conn)
      println("Page " + page + " written to postgres " + " . Time : " + LocalDateTime.now().toString)
      Thread.sleep(5000)
    }
  }

  //ensure that the main thread will not end before each Future has been completed. Timeout after 10 minutes.
  val fSer2: Future[Unit] = for {
    r1 <- f1
    r2 <- f2
  } yield(r1,r2)
  Await.result(fSer2, 10 minutes) //avoid main thread stopping before futures have completed

  // TODO: print message when some pages have not been loaded successfully (due to timeout)?
  fSer2.onComplete{
    case Success(_) =>  {
      println("Finished trying to write pages " + Math.min(hdrPages1.min, hdrPages2.min) + " - " + Math.max(hdrPages1.max, hdrPages2.max) + " to Kafka")
      conn.close()
    }

    case Failure(_) => {
      println("Timeout - not able to complete writing to Kafka within time limit.")
      conn.close()
    }

  }

}
