package kafka

import java.io.{ ByteArrayInputStream }
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ ArrowStreamReader }

object Serdes {

  type BArr = Array[Byte]

  class BytesDeserializer extends AbstractDeserializationSchema[BArr] {
    override def deserialize(bytes: BArr): BArr = bytes
  }

  class ArrowDeserializer extends AbstractDeserializationSchema[ArrowStreamReader] {

    override def deserialize(bytes: BArr): ArrowStreamReader = {
      val alloc  = new RootAllocator(Integer.MAX_VALUE)
      val stream = new ByteArrayInputStream(bytes)
      val reader = new ArrowStreamReader(stream, alloc)

      reader
    }
  }

}
