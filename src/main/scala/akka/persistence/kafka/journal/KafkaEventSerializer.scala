package akka.persistence.kafka.journal

import akka.actor._
import akka.persistence.kafka.Event
import akka.persistence.kafka.journal.EventFormats.{EventDataFormat, EventFormat}
import akka.serialization._

import com.google.protobuf.ByteString

class KafkaEventSerializer(system: ExtendedActorSystem) extends Serializer {
  def identifier: Int = 15443
  def includeManifest: Boolean = false

  def toBinary(o: AnyRef): Array[Byte] = o match {
    case e: Event => eventFormatBuilder(e).build().toByteArray
    case _        => throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass}")
  }

  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef =
    event(EventFormat.parseFrom(bytes))

  def eventFormatBuilder(event: Event): EventFormat.Builder = {
    val builder = EventFormat.newBuilder
    builder.setPersistenceId(event.persistenceId)
    builder.setSequenceNr(event.sequenceNr)
    builder.setData(eventDataFormatBuilder(event.data.asInstanceOf[AnyRef]))
    builder
  }

  def eventDataFormatBuilder(payload: AnyRef): EventDataFormat.Builder = {
    val serializer = SerializationExtension(system).findSerializerFor(payload)
    val builder = EventDataFormat.newBuilder()

    if (serializer.includeManifest)
      builder.setDataManifest(ByteString.copyFromUtf8(payload.getClass.getName))

    builder.setData(ByteString.copyFrom(serializer.toBinary(payload)))
    builder.setSerializerId(serializer.identifier)
    builder
  }

  def event(eventFormat: EventFormat): Event = {
    Event(
      eventFormat.getPersistenceId,
      eventFormat.getSequenceNr,
      eventData(eventFormat.getData))
  }

  def eventData(eventDataFormat: EventDataFormat): Any = {
    val eventDataClass = if (eventDataFormat.hasDataManifest)
      Some(system.dynamicAccess.getClassFor[AnyRef](eventDataFormat.getDataManifest.toStringUtf8).get) else None

    SerializationExtension(system).deserialize(
      eventDataFormat.getData.toByteArray,
      eventDataFormat.getSerializerId,
      eventDataClass).get
  }
}
