package akka.persistence.kafka.journal

import java.io._

import scala.collection.immutable.Seq

import kafka.serializer._
import kafka.utils.VerifiableProperties

case class Event(persistenceId: String, sequenceNr: Long, data: Any)

trait EventTopicMapper {
  def topicsFor(event: Event): Seq[String]
}

class DefaultEventTopicMapper extends EventTopicMapper {
  def topicsFor(event: Event): Seq[String] = List("events")
}

class DefaultEventEncoder(props: VerifiableProperties = null) extends Encoder[Event] {
  def toBytes(event: Event): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(event)
    oos.close()
    bos.toByteArray
  }
}

class DefaultEventDecoder(props: VerifiableProperties = null) extends Decoder[Event] {
  def fromBytes(bytes: Array[Byte]): Event = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    val obj = ois.readObject().asInstanceOf[Event]
    ois.close()
    obj
  }
}

