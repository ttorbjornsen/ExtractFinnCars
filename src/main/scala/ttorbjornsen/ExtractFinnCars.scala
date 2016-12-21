package ttorbjornsen


import kafka.producer.ProducerConfig
import java.util.Properties
import scala.util.Random
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import java.util.Date
import play.api.libs.json._


object ExtractFinnCars extends App {
  val events = 10
  val topic = "finncars_header"
  val brokers = "kafka:9092"
  val rnd = new Random()
  val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "async")
  props.put("request.required.acks", "1")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)

  println("before send")
  val t = System.currentTimeMillis()
  for (nEvents <- Range(0, events)) {
    val runtime = new Date().getTime();
    val ip = "192.168.2." + rnd.nextInt(255);
    val msg = runtime + "," + nEvents + ",www.example.com," + ip;
    val data = new KeyedMessage[String, String](topic, ip, msg);
    producer.send(data);
  }
  println("after send")

  System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()
}
