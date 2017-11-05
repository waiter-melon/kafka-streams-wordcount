package kafka.stream

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}
import spray.json._
import kafka.stream.Producer.{Transaction, TransactionJsonProtocol}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.metrics.stats.Total
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.connect.json.{JsonDeserializer, JsonSerializer}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.streams.kstream._

/**
  * SCALING:
  * You can run n parallel instances of this processor where
  * n <= p where p is the number of topic-partitions
  *
  * Notice that:
  * - Every key change causes a repartition
  * - KStream operates with inserts, KTable operates with upserts (no key duplicates, replacing)
  * - 'aggregate()' in a KGroupedStream is basically foldRight. In KGroupedTable you need a subtractor as well.
  * - 'reduce()' is like foldRight but withouth aggregator and you can't change the type of the input
  */
object StreamsApp extends App {

  val props = new Properties()

  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  /** This guarantees exactly once delivery!!! **/
  props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

  val config: Properties = { props }

  //total balance per user
  //latest transaction time
  //number of transactions

  val INPUT_TOPIC = "transaction-input"
  val OUTPUT_TOPIC = "transaction-output"

  val builder: KStreamBuilder = new KStreamBuilder()

  //case class Entry(balance: Integer, lastTransactionTime: String, transactionsNo: Integer)

  val jsonSerde: Serde[JsonNode] = Serdes.serdeFrom(new JsonSerializer, new JsonDeserializer)
  val recordsStream: KStream[String, JsonNode] = builder.stream(Serdes.String(), jsonSerde, INPUT_TOPIC)

  // initializer
  val initialBalance: ObjectNode = JsonNodeFactory.instance.objectNode()
  initialBalance.put("count", 0)
  initialBalance.put("balance", 0)
  initialBalance.put("time", "-")

  // create the key from the user and write it into Kafka
  recordsStream
    //.mapValues[Transaction](value => deserialize(value))
    .groupByKey(Serdes.String(), jsonSerde)
    .aggregate(() => initialBalance, (key: String, curr: JsonNode, aggrValue: JsonNode) => newBalance(curr, aggrValue), jsonSerde, "reduced-store")
    .to(Serdes.String(), jsonSerde, OUTPUT_TOPIC)

  def newBalance(transaction: JsonNode, balance: JsonNode): JsonNode = {
    val balance: ObjectNode = JsonNodeFactory.instance.objectNode()
    balance.put("count", balance.get("count").asInt + 1)
    balance.put("balance", balance.get("amount").asInt + transaction.get("amount").asInt)
    balance.put("time", "now")
    balance
  }

  val streams = new KafkaStreams(builder, config)

  streams.start()

  println(streams)

  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    streams.close(5, TimeUnit.SECONDS)
  }))

  import TransactionJsonProtocol._
  def deserialize(str: String): Transaction = str.parseJson.convertTo[Transaction]

}
