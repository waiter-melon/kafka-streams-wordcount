package kafka.stream

import java.util.Date

import cakesolutions.kafka.{KafkaProducer, KafkaProducerRecord}
import cakesolutions.kafka.KafkaProducer.Conf

import spray.json._

import org.apache.kafka.common.serialization.StringSerializer

import scala.util.{Failure, Random, Success}

/**
  * Kafka Producer
  */
object Producer extends App {

  case class Transaction(amount: Int, time: String)

  val TOPIC = "transactions-input"
  val TRANS_PER_SECOND = 100

  import scala.concurrent.ExecutionContext.Implicits.global

  val users = List("John", "Stephane", "Alice")

  val configuration = Conf(new StringSerializer(), new StringSerializer(),
    bootstrapServers = "localhost:9092", acks = "all", retries = 3, lingerMs = 1,
    // ensures that we don't push duplicates
    enableIdempotence = true)

  val producer = KafkaProducer[String, String](configuration)

  while(true) {

    import TransactionJsonProtocol._

    val transaction = Transaction(Random.nextInt(100), new Date().toString)
    val json = transaction.toJson.compactPrint

    for (i <- users.indices) {
      val record = KafkaProducerRecord(TOPIC, users(i), json)
      producer.send(record)
        .onComplete {
          case Success(r) => println("offset: " + r.offset + " partition: " + r.partition)
          case Failure(e) => e.printStackTrace()
        }

      Thread.sleep(TRANS_PER_SECOND)
    }
  }

  producer.close()

  object TransactionJsonProtocol extends DefaultJsonProtocol {
    implicit val transactionFormat: RootJsonFormat[Transaction] = jsonFormat(Transaction, "amount", "date")
  }

}