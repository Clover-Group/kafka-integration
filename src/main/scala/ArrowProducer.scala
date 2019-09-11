package kafka

import java.util.Properties
import org.apache.flink.streaming.api.scala.{ StreamExecutionEnvironment, _ }
import org.apache.flink.streaming.connectors.kafka.{ FlinkKafkaConsumer, FlinkKafkaProducer }

import org.apache.flink.api.common.restartstrategy.RestartStrategies

import org.apache.arrow.vector.types.Types.MinorType.{ BIGINT, FLOAT8, VARCHAR }

import org.apache.arrow.vector.ipc.{ ArrowStreamReader }
import kafka.Serdes._
import org.apache.flink.api.common.serialization.SimpleStringSchema

object ArrowProducer extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setRestartStrategy(RestartStrategies.noRestart)
  // env.enableCheckpointing(5000)
  // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  // generate a Watermark every second
  env.getConfig.setAutoWatermarkInterval(1000)
  env.setParallelism(1)

  val props = new Properties

  props.setProperty("bootstrap.servers", "localhost:9092")
  props.setProperty("group.id", "group0")
  props.setProperty("auto.offset.reset", "earliest"); // Always read topic from start

  val topic = "arrowTopic"

  val cons = new FlinkKafkaConsumer(topic, new SimpleStringSchema, props)
  val prod = new FlinkKafkaProducer(topic, new SimpleStringSchema, props)

  val stream = env.addSource(cons)
  stream.addSink(prod)

  // prod.

  stream.print
  env.execute
}
