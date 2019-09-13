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

object StringProducer extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setRestartStrategy(RestartStrategies.noRestart)
  // env.enableCheckpointing(5000)
  // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  // generate a Watermark every second
  env.getConfig.setAutoWatermarkInterval(1000)
  env.setParallelism(1)

  val prodProps = new Properties
  
  prodProps.put("bootstrap.servers", "localhost:9092");
  prodProps.put("acks", "all");
  prodProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  prodProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

  val topic = "stringTopic"

  val prod = new KafkaProducer(prodProps)

  val msg = new ProducerRecord[String, String](topic, "hello", "world")

  // for (i <- 0 to 1)
  //   prod.send(msg)


  prod.close

}
