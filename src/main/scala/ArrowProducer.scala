package kafka

import java.util.Properties
import org.apache.flink.streaming.api.scala.{ StreamExecutionEnvironment, _ }
import org.apache.flink.streaming.connectors.kafka.{ FlinkKafkaConsumer, FlinkKafkaProducer }

import org.apache.flink.api.common.restartstrategy.RestartStrategies

import org.apache.arrow.vector.types.Types.MinorType.{ BIGINT, FLOAT8, VARCHAR }

import org.apache.arrow.vector.ipc.{ ArrowStreamReader }
import kafka.Serdes._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
// import org.apache.kafka.common.serialization.By

object ArrowProducer extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setRestartStrategy(RestartStrategies.noRestart)
  // env.enableCheckpointing(5000)
  // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  // generate a Watermark every second
  env.getConfig.setAutoWatermarkInterval(1000)
  env.setParallelism(1)

  val prodProps = new Properties
  val consProps = new Properties

  consProps.setProperty("bootstrap.servers", "localhost:9092")
  consProps.setProperty("group.id", "group0")
  consProps.setProperty("auto.offset.reset", "earliest"); // Always read topic from start

  // prodProps.setProperty("bootstrap.servers", "localhost:9092")
  // prodProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  // prodProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  // prodProps.setProperty("acks", "0")
  // prodProps.setProperty("topic", "stringTopic")
  prodProps.put("bootstrap.servers", "localhost:9092");
  prodProps.put("acks", "all");
  prodProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  prodProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

  val topic = "stringTopic"

  val cons = new FlinkKafkaConsumer(topic, new SimpleStringSchema, consProps)
  val prod = new KafkaProducer[Int, String](prodProps)

  val msg = new ProducerRecord[Int, String]("some key", "some value")
  println("Before Sending the msg")
  prod.flush
  prod.send(msg)
  prod.close
  println("After Sending the msg")

  val stream: DataStream[String] = env.addSource(cons)
  //stream.addSink(prod)

  stream.print
  env.execute
}
