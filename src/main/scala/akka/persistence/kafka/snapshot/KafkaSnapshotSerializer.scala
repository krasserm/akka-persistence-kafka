package akka.persistence.kafka.snapshot

import java.io._

import akka.actor._
import akka.persistence._
import akka.persistence.kafka.snapshot.SnapshotFormats.SnapshotMetadataFormat
import akka.persistence.serialization.Snapshot
import akka.serialization._

case class KafkaSnapshot(metadata: SnapshotMetadata, snapshot: Any) {
  def matches(criteria: SnapshotSelectionCriteria): Boolean =
    metadata.sequenceNr <= criteria.maxSequenceNr &&
      metadata.timestamp <= criteria.maxTimestamp
}

class KafkaSnapshotSerializer(system: ExtendedActorSystem) extends Serializer {
  def identifier: Int = 15442
  def includeManifest: Boolean = false

  def toBinary(o: AnyRef): Array[Byte] = o match {
    case ks: KafkaSnapshot => snapshotToBinary(ks)
    case _                 => throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass}")
  }

  def snapshotToBinary(ks: KafkaSnapshot): Array[Byte] = {
    val extension = SerializationExtension(system)
    val snapshot = Snapshot(ks.snapshot)
    val snapshotSerializer = extension.findSerializerFor(snapshot)

    val snapshotBytes = snapshotSerializer.toBinary(snapshot)
    val metadataBytes = snapshotMetadataToBinary(ks.metadata)

    val out = new ByteArrayOutputStream

    writeInt(out, metadataBytes.length)
    out.write(metadataBytes)
    out.write(snapshotBytes)
    out.toByteArray
  }

  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): KafkaSnapshot = {
    val extension = SerializationExtension(system)
    val metadataLength = readInt(new ByteArrayInputStream(bytes))
    val metadataBytes = bytes.slice(4, metadataLength + 4)
    val snapshotBytes = bytes.drop(metadataLength + 4)

    val metadata = snapshotMetadataFromBinary(metadataBytes)
    val snapshot = extension.deserialize(snapshotBytes, classOf[Snapshot]).get

    KafkaSnapshot(metadata, snapshot.data)
  }

  def snapshotMetadataToBinary(metadata: SnapshotMetadata): Array[Byte] = {
    SnapshotMetadataFormat.newBuilder()
      .setPersistenceId(metadata.persistenceId)
      .setSequenceNr(metadata.sequenceNr)
      .setTimestamp(metadata.timestamp)
      .build()
      .toByteArray
  }

  def snapshotMetadataFromBinary(metadataBytes: Array[Byte]): SnapshotMetadata = {
    val md = SnapshotMetadataFormat.parseFrom(metadataBytes)
    SnapshotMetadata(
      md.getPersistenceId,
      md.getSequenceNr,
      md.getTimestamp)
  }

  private def writeInt(outputStream: OutputStream, i: Int) =
    0 to 24 by 8 foreach { shift ⇒ outputStream.write(i >> shift) }

  private def readInt(inputStream: InputStream) =
    (0 to 24 by 8).foldLeft(0) { (id, shift) ⇒ (id | (inputStream.read() << shift)) }
}
