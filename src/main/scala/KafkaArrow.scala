package kafka

import java.util.Properties
import org.apache.flink.streaming.api.scala.{ StreamExecutionEnvironment, _ }
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import org.apache.flink.api.common.restartstrategy.RestartStrategies

import org.apache.arrow.vector.types.Types.MinorType.{ BIGINT, FLOAT8, VARCHAR }

import org.apache.arrow.vector.ipc.{ ArrowStreamReader }
import kafka.Serdes._

object KafkaArrow extends App {

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

  val topic    = "arrowTopic"
  val consumer = new FlinkKafkaConsumer(topic, new ArrowDeserializer, props)

  val stream: DataStream[ArrowStreamReader] = env.addSource(consumer)

  val out: DataStream[Unit] = stream.map(str => {

    while (str.loadNextBatch) {

      val root = str.getVectorSchemaRoot
      root.getSchema
      val vectors = root.getFieldVectors

      println(s"Total vectors = ${vectors.size}")

      vectors.forEach(
        v =>
          v.getMinorType match {
            case BIGINT  => println("BIGINT found")
            case VARCHAR => println("VARCHAR found")
            case FLOAT8  => println("FLOAT8 found")
            case unknown => println(s"Unknown vector type: $unknown")
          }
      )
    }
  })

  env.execute()
}
