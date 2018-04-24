package ttorbjornsen

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.HashMap
import org.postgresql.util.PGobject

import kafka.producer.{KeyedMessage, Producer}
import org.jsoup.Jsoup
import org.jsoup.nodes._
import play.api.libs.json._
import ttorbjornsen.ExtractFinnCars.conn

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

  def saveFinnCarsPageResults(producer:Producer[String,String], topic:String, url: String, conn: Connection):Unit = {
    //val pageResult = Future{Source.fromURL(new URL(url)).mkString}
    val pageResult = Future{scrapeCarHeadersFromPage(url).toString}

    val action = pageResult.map {results =>
      //producer.send(new KeyedMessage[String, String](topic, results))
      try {
        // Configure to be Read Only
        //val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        val insertStatement = "INSERT INTO acq_car_hdr(url, json) VALUES (?,?)"
        val preparedStatement = conn.prepareStatement(insertStatement)
        preparedStatement.setString(1, url)
        val jsonObject = new PGobject()
        jsonObject.setType("json")
        jsonObject.setValue(results)
        //jsonObject.setValue("[{\"finnkode\":\"114270330\",\"location\":\"Notodden\",\"title\":\"Opel Insignia 2,0 CDTi 130hk Aut Cosmo S-tourer\",\"year\":\"2011\",\"km\":\"261 000 km\",\"price\":\"60 750,-\"},{\"finnkode\":\"118830196\",\"location\":\"Hommelvik\",\"title\":\"Mercedes-Benz E-Klasse E200 CDI T aut\",\"year\":\"2013\",\"km\":\"338 653 km\",\"price\":\"188 719,-\"},{\"finnkode\":\"118751752\",\"location\":\"Elvarli\",\"title\":\"Toyota Avensis AVENSIS 2.0-126 D\",\"year\":\"2008\",\"km\":\"162 000 km\",\"price\":\"59 719,-\"},{\"finnkode\":\"118828679\",\"location\":\"Arendal\",\"title\":\"Mazda 6 2.0-140hk, CD Diesel, EU 05.2020, h.feste,\",\"year\":\"2008\",\"km\":\"199 000 km\",\"price\":\"57 000,-\"},{\"finnkode\":\"118806754\",\"location\":\"Kråkerøy\",\"title\":\"Volvo V50 DRIVe Limited Edition start/stop\",\"year\":\"2012\",\"km\":\"116 407 km\",\"price\":\"108 719,-\"},{\"finnkode\":\"111317324\",\"location\":\"Ålesund\",\"title\":\"Audi A6 allroad quattro 3.0 TDI V6 211hk S tronic Navi-PLUS Xenon Krok+\",\"year\":\"2016\",\"km\":\"18 000 km\",\"price\":\"644 000,-\"},{\"finnkode\":\"116313435\",\"location\":\"Ålesund\",\"title\":\"Audi A6 allroad quattro 3.0 TDI 204hk S tronic Navi Krok Skinn++\",\"year\":\"2013\",\"km\":\"20 800 km\",\"price\":\"489 000,-\"},{\"finnkode\":\"118823971\",\"location\":\"Frolands Verk\",\"title\":\"Skoda Octavia 5E\",\"year\":\"2014\",\"km\":\"57 000 km\",\"price\":\"265 000,-\"},{\"finnkode\":\"118828961\",\"location\":\"Steinkjer\",\"title\":\"Audi A6 allroad A6 ALLR. 2.7-190 D Q\",\"year\":\"2010\",\"km\":\"219 000 km\",\"price\":\"198 719,-\"},{\"finnkode\":\"118729812\",\"location\":\"Sollihøgda\",\"title\":\"Skoda Fabia 1,2 65hk Ambiente\",\"year\":\"2007\",\"km\":\"149 200 km\",\"price\":\"20 656,-\"},{\"finnkode\":\"118829480\",\"location\":\"Trondheim\",\"title\":\"Volkswagen Golf 105 TDi Highline stv | TLF | Park.sensorer | Norsk bil\",\"year\":\"2012\",\"km\":\"99 800 km\",\"price\":\"119 000,-\"},{\"finnkode\":\"118824796\",\"location\":\"Averøy\",\"title\":\"BMW 5-serie 530D XDRIVE 3.0-258D\",\"year\":\"2011\",\"km\":\"176 000 km\",\"price\":\"333 719,-\"},{\"finnkode\":\"118823867\",\"location\":\"Bjørkelangen\",\"title\":\"Mercedes-Benz C-Klasse C180 2xAMG///BURMEISTER,WEBASTO,ILS LED,SKINN,KROK,PANO\",\"year\":\"2014\",\"km\":\"86 000 km\",\"price\":\"352 719,-\"},{\"finnkode\":\"116722923\",\"location\":\"Sunde i Sunnhordland\",\"title\":\"BMW 5-serie 530D XDRIVE 3.0-258D\",\"year\":\"2011\",\"km\":\"114 000 km\",\"price\":\"413 719,-\"},{\"finnkode\":\"118827956\",\"location\":\"Nykirke\",\"title\":\"Mercedes-Benz C-Klasse C200T CDI Avantgarde AMG\",\"year\":\"2008\",\"km\":\"250 000 km\",\"price\":\"82 719,-\"},{\"finnkode\":\"116580946\",\"location\":\"Mandal\",\"title\":\"Peugeot 307 SW 307 1.6-90 D SW\",\"year\":\"2007\",\"km\":\"153 000 km\",\"price\":\"26 719,-\"},{\"finnkode\":\"118828358\",\"location\":\"Tromsø\",\"title\":\"Ford Focus 1,5 TDCi 120hk Titanium X aut Nøkkelfri/ NAVI/ DAB/ +++\",\"year\":\"2016\",\"km\":\"61 000 km\",\"price\":\"269 900,-\"},{\"finnkode\":\"118808049\",\"location\":\"Vollen\",\"title\":\"Skoda Octavia 1,4 TSI 140hk Ambition 7DSG\",\"year\":\"2014\",\"km\":\"85 000 km\",\"price\":\"203 719,-\"},{\"finnkode\":\"118822180\",\"location\":\"Sundlandet\",\"title\":\"Volvo V50 1,6 D\",\"year\":\"2008\",\"km\":\"137 378 km\",\"price\":\"85 219,-\"},{\"finnkode\":\"118785843\",\"location\":\"Gardermoen\",\"title\":\"Audi A4 2.0 TFSI quattro sport 220 hk\",\"year\":\"2007\",\"km\":\"170 000 km\",\"price\":\"175 594,-\"},{\"finnkode\":\"118825543\",\"location\":\"Rjukan\",\"title\":\"Volvo V60 Volvo D2 DRIVe R-Design\",\"year\":\"2012\",\"km\":\"153 000 km\",\"price\":\"133 719,-\"},{\"finnkode\":\"118822758\",\"location\":\"Ulsteinvik\",\"title\":\"Volkswagen Passat 2,0 CR TDI Sportline\",\"year\":\"2008\",\"km\":\"168 000 km\",\"price\":\"68 719,-\"},{\"finnkode\":\"118821121\",\"location\":\"Ålesund\",\"title\":\"Audi A6 allroad 3,0 TDI quattro tiptronic\",\"year\":\"2007\",\"km\":\"193 300 km\",\"price\":\"204 594,-\"},{\"finnkode\":\"118827387\",\"location\":\"Mjøndalen\",\"title\":\"Ford Mondeo NYPRIS/ 1,8 L DIESEL/ NY EU/ TITANIUM UTGAVE!\",\"year\":\"2009\",\"km\":\"147 000 km\",\"price\":\"59 719,-\"},{\"finnkode\":\"118337208\",\"location\":\"Oslo\",\"title\":\"Mercedes-Benz CLS CLS 350 CDI 4M SB AMG/DIST/KEYLESS/AIRMATIC/COMAND/DAB\",\"year\":\"2013\",\"km\":\"73 000 km\",\"price\":\"483 719,-\"},{\"finnkode\":\"118826136\",\"location\":\"Skjold\",\"title\":\"Opel Combo COMBO 1.2-75 D\",\"year\":\"2011\",\"km\":\"73 000 km\",\"price\":\"53 775,-\"},{\"finnkode\":\"108965060\",\"location\":\"Oslo\",\"title\":\"Volkswagen Golf 1,4 TSI 140hk Highline. Automat/DSG7. Topp utstyrt!\",\"year\":\"2014\",\"km\":\"66 000 km\",\"price\":\"219 900,-\"},{\"finnkode\":\"118815883\",\"location\":\"Seljord\",\"title\":\"Volkswagen Caddy CADDY DIESEL, AUTOMAT, MEGET PEN BIl , KUN 43000KM !!!\",\"year\":\"2012\",\"km\":\"43 230 km\",\"price\":\"129 000,-\"},{\"finnkode\":\"118803197\",\"location\":\"Vøyenenga\",\"title\":\"Subaru Forester FORESTER 2.0-150 4WD\",\"year\":\"2009\",\"km\":\"212 000 km\",\"price\":\"79 719,-\"},{\"finnkode\":\"118825757\",\"location\":\"Hobøl\",\"title\":\"Opel Vectra 1.8i aut Cosmo OPC-Line (sjelden hel og pen)\",\"year\":\"2007\",\"km\":\"183 600 km\",\"price\":\"52 660,-\"},{\"finnkode\":\"118825717\",\"location\":\"Porsgrunn\",\"title\":\"Audi A4 AVANT 1.8TFSi *FJERNSTYRT PARK VARMER * NAVI * CRUISE\",\"year\":\"2013\",\"km\":\"107 000 km\",\"price\":\"188 900,-\"},{\"finnkode\":\"118821948\",\"location\":\"Tromsø\",\"title\":\"Subaru Forester FORESTER 2.0-137 4WD\",\"year\":\"2007\",\"km\":\"87 180 km\",\"price\":\"111 594,-\"},{\"finnkode\":\"101973845\",\"location\":\"Tranby\",\"title\":\"Opel Astra 1,6 Cosmo Easy\",\"year\":\"2009\",\"km\":\"173 600 km\",\"price\":\"60 000,-\"},{\"finnkode\":\"118819177\",\"location\":\"Oslo\",\"title\":\"Volkswagen Passat 2,0 TDI Highline 6-trinn DSG\",\"year\":\"2007\",\"km\":\"135 843 km\",\"price\":\"48 594,-\"},{\"finnkode\":\"118823697\",\"location\":\"Hokksund\",\"title\":\"Audi A4 2.0TDI 150HK S-LINE QUATTRO+SKINN+XENON+KROK+NAVI+DAB++\",\"year\":\"2015\",\"km\":\"100 000 km\",\"price\":\"277 719,-\"},{\"finnkode\":\"118820238\",\"location\":\"Rjukan\",\"title\":\"Volkswagen Passat 2,0 TDI 140hk BMT Exclusive R DSG\",\"year\":\"2013\",\"km\":\"148 000 km\",\"price\":\"173 719,-\"},{\"finnkode\":\"118823347\",\"location\":\"Høvik\",\"title\":\"Hyundai i40 1.7 CRDI/EXECUTIVE/SKINN/KAMERA/CRUISE/NYBILGARANTI\",\"year\":\"2013\",\"km\":\"54 348 km\",\"price\":\"169 900,-\"},{\"finnkode\":\"111878792\",\"location\":\"Nesttun\",\"title\":\"Volvo V60 T3 R-design, bensin\",\"year\":\"2012\",\"km\":\"93 000 km\",\"price\":\"203 719,-\"},{\"finnkode\":\"118759130\",\"location\":\"Nittedal\",\"title\":\"Saab 9-3 1,9TiD PF SportCombi Vector\",\"year\":\"2008\",\"km\":\"182 045 km\",\"price\":\"73 719,-\"},{\"finnkode\":\"118813727\",\"location\":\"Solbergelva\",\"title\":\"Opel Insignia 2,0 CDTi 130hk Cosmo Sportstourer\",\"year\":\"2009\",\"km\":\"140 900 km\",\"price\":\"89 719,-\"},{\"finnkode\":\"118817725\",\"location\":\"Hønefoss\",\"title\":\"Skoda Superb 2,0 TDI CR DSG Elegance DPF M/DELSKINN OG WEBASTO\",\"year\":\"2010\",\"km\":\"140 000 km\",\"price\":\"139 999,-\"},{\"finnkode\":\"118820645\",\"location\":\"Høvik\",\"title\":\"Audi A6 allroad 3.0 TDI/NAF-TESTET/XENONPLUS/NAVI/ADAPTIV CRUISE/KROK\",\"year\":\"2013\",\"km\":\"69 750 km\",\"price\":\"429 900,-\"},{\"finnkode\":\"118710600\",\"location\":\"Fannrem\",\"title\":\"Volkswagen Passat PASSAT 2.0-110 D\",\"year\":\"2010\",\"km\":\"131 300 km\",\"price\":\"53 719,-\"},{\"finnkode\":\"118812553\",\"location\":\"Hokksund\",\"title\":\"Volkswagen Passat 120HK TDI WEBASTO+XENON/LED+ERGO SPORTSSETER+KROK+DAB++\",\"year\":\"2015\",\"km\":\"142 000 km\",\"price\":\"198 861,-\"},{\"finnkode\":\"89711250\",\"location\":\"Lillesand\",\"title\":\"BMW 3-serie\",\"year\":\"2012\",\"km\":\"114 000 km\",\"price\":\"249 660,-\"},{\"finnkode\":\"66552658\",\"location\":\"Drammen\",\"title\":\"Mazda 6 1,8 120hk AdvancePlus\",\"year\":\"2008\",\"km\":\"177 600 km\",\"price\":\"70 500,-\"},{\"finnkode\":\"118809284\",\"location\":\"Elverum\",\"title\":\"Skoda Superb 2,0 TDI 170hk 4x4 L&K DSG\",\"year\":\"2014\",\"km\":\"68 500 km\",\"price\":\"332 719,-\"},{\"finnkode\":\"118816392\",\"location\":\"Askim\",\"title\":\"Audi A4 2,0 multitronic\",\"year\":\"2008\",\"km\":\"191 577 km\",\"price\":\"75 000,-\"},{\"finnkode\":\"90460097\",\"location\":\"Oslo\",\"title\":\"Toyota Prius+ Seven TOPPMODELL - PREMIUM - STRØKEN. EU-Godkjent\",\"year\":\"2013\",\"km\":\"60 500 km\",\"price\":\"250 000,-\"},{\"finnkode\":\"113148964\",\"location\":\"Namsos\",\"title\":\"BMW 3-serie\",\"year\":\"2007\",\"km\":\"167 258 km\",\"price\":\"200 000,-\"},{\"finnkode\":\"117725109\",\"location\":\"Jar\",\"title\":\"Toyota Auris Touring Sports 1,8 Hybrid Executive\",\"year\":\"2015\",\"km\":\"32 150 km\",\"price\":\"225 000,-\"},{\"finnkode\":\"118811110\",\"location\":\"Fredrikstad\",\"title\":\"Mercedes-Benz E-Klasse E220 CDI T Avantgarde aut. Navi/krok/EL bakluke\",\"year\":\"2012\",\"km\":\"159 871 km\",\"price\":\"239 900,-\"},{\"finnkode\":\"118815573\",\"location\":\"Molde\",\"title\":\"BMW 5-serie xDrive / Hifi / Navi / Krok / Dab+ / Kald Klima\",\"year\":\"2016\",\"km\":\"33 000 km\",\"price\":\"415 000,-\"},{\"finnkode\":\"109554184\",\"location\":\"Bardu\",\"title\":\"Mercedes-Benz C-Klasse C200T CDI Classic aut.\",\"year\":\"2011\",\"km\":\"116 000 km\",\"price\":\"150 000,-\"},{\"finnkode\":\"118812613\",\"location\":\"Skien\",\"title\":\"Citroen Berlingo BERLINGO 1.6-75 D EU GODKJENT TIL APRIL 2020 pen bil\",\"year\":\"2008\",\"km\":\"149 950 km\",\"price\":\"26 275,-\"},{\"finnkode\":\"118816102\",\"location\":\"Grong\",\"title\":\"Volkswagen Passat 1,6 Tdi 120hk Comfortline\",\"year\":\"2015\",\"km\":\"75 581 km\",\"price\":\"229 900,-\"},{\"finnkode\":\"118814489\",\"location\":\"Ålesund\",\"title\":\"Volvo V70 R-Design Xenon Skinn Navi Soltak Krok Sensor Foran Bak\",\"year\":\"2012\",\"km\":\"102 000 km\",\"price\":\"149 719,-\"},{\"finnkode\":\"118816142\",\"location\":\"Grong\",\"title\":\"Volkswagen Passat 1,6 Tdi 120hk Comfortline\",\"year\":\"2015\",\"km\":\"66 831 km\",\"price\":\"229 900,-\"},{\"finnkode\":\"118815869\",\"location\":\"Grong\",\"title\":\"Volkswagen Passat PASSAT 1,6 TDI 105HK DSG BUSINESS EDITION\",\"year\":\"2014\",\"km\":\"110 861 km\",\"price\":\"199 900,-\"},{\"finnkode\":\"118815903\",\"location\":\"Grong\",\"title\":\"Volvo XC 70 D3 163hk Summum\",\"year\":\"2011\",\"km\":\"180 675 km\",\"price\":\"199 900,-\"},{\"finnkode\":\"118815940\",\"location\":\"Grong\",\"title\":\"Skoda Octavia 1,4 TSi 140hk Elegance\",\"year\":\"2014\",\"km\":\"63 682 km\",\"price\":\"209 900,-\"},{\"finnkode\":\"118815723\",\"location\":\"Grong\",\"title\":\"Skoda Octavia 1,9 TDi 105hk 4x4\",\"year\":\"2008\",\"km\":\"163 894 km\",\"price\":\"129 900,-\"},{\"finnkode\":\"118815595\",\"location\":\"Svolvær\",\"title\":\"Toyota Avensis 2,0 D-4D DPF 126hk Comfort DAB/NAVI/ISOFIX/HENGERFESTE\",\"year\":\"2011\",\"km\":\"109 084 km\",\"price\":\"129 900,-\"},{\"finnkode\":\"118806167\",\"location\":\"Skien\",\"title\":\"Toyota Avensis 2.0-126 D XENON/KROK/NAVI/ NY MOTOR/ GARANTI\",\"year\":\"2007\",\"km\":\"209 000 km\",\"price\":\"49 719,-\"},{\"finnkode\":\"118815304\",\"location\":\"Svolvær\",\"title\":\"Volvo V60 D5 AWD Summum Plug in Hybrid Driversupport/VOC/Kamera\",\"year\":\"2015\",\"km\":\"43 000 km\",\"price\":\"399 900,-\"},{\"finnkode\":\"118813061\",\"location\":\"Krokstadelva\",\"title\":\"Volkswagen Passat 2,0 CR TDI Highline 4Motion\",\"year\":\"2008\",\"km\":\"253 000 km\",\"price\":\"45 719,-\"},{\"finnkode\":\"118812387\",\"location\":\"Tønsberg\",\"title\":\"Volvo V50 1,6 D\",\"year\":\"2008\",\"km\":\"116 000 km\",\"price\":\"65 000,-\"},{\"finnkode\":\"118812699\",\"location\":\"Oslo\",\"title\":\"Volvo V60 D3 Summum aut\",\"year\":\"2011\",\"km\":\"158 000 km\",\"price\":\"168 719,-\"},{\"finnkode\":\"118814224\",\"location\":\"Tiller\",\"title\":\"Honda Accord 2,2 i-DTEC Elegance Aut. 17\\\"Alu.felger/Aut.klima/DAB+\",\"year\":\"2009\",\"km\":\"139 500 km\",\"price\":\"115 000,-\"},{\"finnkode\":\"118789512\",\"location\":\"Trondheim\",\"title\":\"Volvo V70 D3 automat\",\"year\":\"2014\",\"km\":\"109 000 km\",\"price\":\"259 900,-\"},{\"finnkode\":\"118811294\",\"location\":\"Rolvsøy\",\"title\":\"Toyota Auris TOURING SPORTS 1.8-99 HYBRID ACTIVE+\",\"year\":\"2015\",\"km\":\"45 400 km\",\"price\":\"221 161,-\"},{\"finnkode\":\"118813244\",\"location\":\"Namsos\",\"title\":\"Volvo V70 D3 E Summum aut VOC,\",\"year\":\"2013\",\"km\":\"102 500 km\",\"price\":\"279 900,-\"},{\"finnkode\":\"118810930\",\"location\":\"Olden\",\"title\":\"Volvo V70 D3 Summum aut\",\"year\":\"2011\",\"km\":\"97 000 km\",\"price\":\"217 719,-\"},{\"finnkode\":\"118810929\",\"location\":\"Revetal\",\"title\":\"Volkswagen Passat PASSAT 1.6-105 D Highline\",\"year\":\"2011\",\"km\":\"185 650 km\",\"price\":\"78 719,-\"},{\"finnkode\":\"116447892\",\"location\":\"Voss\",\"title\":\"Volkswagen Caddy CADDY 1.9-105 D 4Motion, henger feste\",\"year\":\"2010\",\"km\":\"105 500 km\",\"price\":\"54 900,-\"},{\"finnkode\":\"106017160\",\"location\":\"Sjetnemarka\",\"title\":\"Mazda 6 2,0 145hk Vision\",\"year\":\"2014\",\"km\":\"200 500 km\",\"price\":\"164 064,-\"},{\"finnkode\":\"118805547\",\"location\":\"Arendal\",\"title\":\"Volkswagen Passat Volkswagen Passat 2015 2.0TDI 150HK 4Motion Highline\",\"year\":\"2015\",\"km\":\"52 000 km\",\"price\":\"361 166,-\"},{\"finnkode\":\"68295854\",\"location\":\"Tananger\",\"title\":\"Honda Accord 2009 model 2.0i EXECUTIVE , NAVI,\",\"year\":\"2008\",\"km\":\"159 000 km\",\"price\":\"99 500,-\"},{\"finnkode\":\"118804630\",\"location\":\"Grålum\",\"title\":\"Audi A4 2,0 TDI 120 hk\",\"year\":\"2011\",\"km\":\"160 000 km\",\"price\":\"158 719,-\"},{\"finnkode\":\"106662067\",\"location\":\"Ingeberg\",\"title\":\"Mercedes-Benz E-Klasse E220 CDI T AVANTGARDE LINE AUT, FACELIFT S212\",\"year\":\"2014\",\"km\":\"97 000 km\",\"price\":\"349 719,-\"},{\"finnkode\":\"118810673\",\"location\":\"Namsos\",\"title\":\"Volvo V70 D2 Dynamic Edition aut 119g Volvo OnCall, DAB+, Skinn++\",\"year\":\"2015\",\"km\":\"42 500 km\",\"price\":\"329 900,-\"},{\"finnkode\":\"118806985\",\"location\":\"Slemmestad\",\"title\":\"Volkswagen Passat 1,9 TDI Trendline\",\"year\":\"2007\",\"km\":\"236 525 km\",\"price\":\"38 000,-\"},{\"finnkode\":\"118802302\",\"location\":\"Hunndalen\",\"title\":\"Ford Mondeo 2,0 130hk TDCI Titanium X aut.\",\"year\":\"2008\",\"km\":\"153 325 km\",\"price\":\"98 719,-\"},{\"finnkode\":\"118791713\",\"location\":\"Halden\",\"title\":\"Toyota Avensis 2,0 D-4D Executive m/DPF\",\"year\":\"2008\",\"km\":\"190 000 km\",\"price\":\"49 999,-\"},{\"finnkode\":\"118664159\",\"location\":\"Sandnes\",\"title\":\"Mazda 6 2.2 D 129HK AdvancePlus - DAB+/Hengerfeste/Xenon/Cruise\",\"year\":\"2011\",\"km\":\"141 000 km\",\"price\":\"128 719,-\"},{\"finnkode\":\"118809582\",\"location\":\"Hamar\",\"title\":\"Ford Focus 2,0 TDCi 115hk Titanium Aut.\",\"year\":\"2013\",\"km\":\"83 738 km\",\"price\":\"169 900,-\"},{\"finnkode\":\"118806980\",\"location\":\"Langhus\",\"title\":\"Toyota Avensis 2.0D4D,Xenon,Garanti,Eu ok2020,Hengerfeste,Navi,Webasto\",\"year\":\"2008\",\"km\":\"170 000 km\",\"price\":\"61 999,-\"},{\"finnkode\":\"118809243\",\"location\":\"Hamar\",\"title\":\"Ford Mondeo 1,6 TDCI 115hk Titanium Styling /Webasto/Tilhengerfeste\",\"year\":\"2013\",\"km\":\"64 000 km\",\"price\":\"189 900,-\"},{\"finnkode\":\"118804338\",\"location\":\"Rendalen\",\"title\":\"Toyota Avensis AVENSIS 2.0 D-4D EXECUTIVE, SKINN,\",\"year\":\"2009\",\"km\":\"150 000 km\",\"price\":\"97 000,-\"},{\"finnkode\":\"118808628\",\"location\":\"Oslo\",\"title\":\"Audi A6 Avant 2,0 TDI 190hk quattro S tronic Panorama, Webasto\",\"year\":\"2017\",\"km\":\"24 000 km\",\"price\":\"579 000,-\"},{\"finnkode\":\"118805995\",\"location\":\"Dønna\",\"title\":\"Skoda Octavia OCTAVIA 1.9-105 D 4X\",\"year\":\"2007\",\"km\":\"200 000 km\",\"price\":\"60 000,-\"},{\"finnkode\":\"118804705\",\"location\":\"Sola\",\"title\":\"Volkswagen Passat 2,0 TDI Comfortline\",\"year\":\"2007\",\"km\":\"359 km\",\"price\":\"30 719,-\"},{\"finnkode\":\"118807865\",\"location\":\"Bosberg\",\"title\":\"Volkswagen Passat *2.0*EXCLUSIVE*R-LINE*SKINN*NAVI*WEBASTO*MEGET PEN*\",\"year\":\"2013\",\"km\":\"96 240 km\",\"price\":\"219 000,-\"},{\"finnkode\":\"118807515\",\"location\":\"Tromsø\",\"title\":\"Volvo V90 T8 407hk R-Design AWD aut\",\"year\":\"2018\",\"km\":\"8 000 km\",\"price\":\"4 999,-\"},{\"finnkode\":\"118805566\",\"location\":\"Kongsberg\",\"title\":\"Volkswagen Passat Alltrack 4M Executive TDI 190hk DSG Skinn Webasto H.feste DAB+\",\"year\":\"2016\",\"km\":\"44 000 km\",\"price\":\"429 000,-\"},{\"finnkode\":\"118798918\",\"location\":\"Stabekk\",\"title\":\"Audi A4 allroad 2,0 143HK TDI SKINN/4X4/EU-GOKJ T JAN 2020/TOPP UTSTYRT\",\"year\":\"2010\",\"km\":\"172 000 km\",\"price\":\"147 900,-\"},{\"finnkode\":\"118806575\",\"location\":\"Ulset\",\"title\":\"Volvo V60 D2 Momentum 119g aut Ryggekamera-dab-kamp.40.000,-\",\"year\":\"2015\",\"km\":\"52 300 km\",\"price\":\"279 000,-\"},{\"finnkode\":\"118806224\",\"location\":\"Sola\",\"title\":\"Volvo V60 D2 m/Klima/PDC/Bluetooth/Skiboks\",\"year\":\"2012\",\"km\":\"135 000 km\",\"price\":\"148 000,-\"},{\"finnkode\":\"118805963\",\"location\":\"Rødberg\",\"title\":\"Volkswagen Golf 1,9 TDI 105hk Comfortline\",\"year\":\"2009\",\"km\":\"146 000 km\",\"price\":\"75 000,-\"},{\"finnkode\":\"118805951\",\"location\":\"Halden\",\"title\":\"Ford Mondeo 2.0 TDCi Titanium X /Navi/Keyless/PDC/Isofix/Xenon\",\"year\":\"2009\",\"km\":\"173 678 km\",\"price\":\"89 000,-\"},{\"finnkode\":\"118784061\",\"location\":\"Askim\",\"title\":\"Volvo V50 2.0D AUT SUMMUM PARK.VARMER SKINN MLF BLUETOOTH ALARM\",\"year\":\"2009\",\"km\":\"169 900 km\",\"price\":\"84 000,-\"}]")
        preparedStatement.setObject(2, jsonObject)



        // Execute Query
        val rs = preparedStatement.executeUpdate()
      }

      println("success - extracting url " + url + results)
    } recover{
      case t: Throwable => {
        t.printStackTrace()
//        println("failure - extracting url " + url)
        saveFinnCarsPageResults(producer, topic, url, conn) //retry

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
