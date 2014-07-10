package akka.persistence.kafka.journal

import java.util.Properties

import scala.collection.immutable.Seq

import akka.actor._
import akka.persistence.{PersistentId, PersistentConfirmation, PersistentRepr}
import akka.persistence.journal.SyncWriteJournal
import akka.serialization.SerializationExtension

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

class KafkaJournal extends SyncWriteJournal with KafkaRecovery {
  val config = context.system.settings.config.getConfig("kafka-journal")
  val serialization = SerializationExtension(context.system)

  val msgProducerProps = new Properties()
  val evtProducerProps = new Properties()

  val watermarks: ActorRef = context.actorOf(Props[Watermarks])
  var deletions = Map.empty[String, (Long, Boolean)] // TODO: make persistent

  msgProducerProps.put("metadata.broker.list", "localhost:6667")
  msgProducerProps.put("producer.type", "sync")
  msgProducerProps.put("request.required.acks", "1")
  msgProducerProps.put("key.serializer.class", "kafka.serializer.StringEncoder")
  msgProducerProps.put("partitioner.class", "akka.persistence.kafka.journal.StickyPartitioner")

  evtProducerProps.put("metadata.broker.list", "localhost:6667")
  evtProducerProps.put("producer.type", "async")
  evtProducerProps.put("serializer.class", "akka.persistence.kafka.journal.DefaultEventEncoder")
  evtProducerProps.put("key.serializer.class", "kafka.serializer.StringEncoder")

  val msgProducer = new Producer[String, Array[Byte]](new ProducerConfig(msgProducerProps))
  val evtProducer = new Producer[String, Event](new ProducerConfig(evtProducerProps))
  val evtTopicMapper = new DefaultEventTopicMapper()

  def writeMessages(messages: Seq[PersistentRepr]): Unit = {
    val keyedMsgs = for {
      m <- messages
    } yield new KeyedMessage[String, Array[Byte]](m.persistenceId, "static", serialization.serialize(m).get)

    val keyedEvents = for {
      m <- messages
      e = Event(m.persistenceId, m.sequenceNr, m.payload)
      t <- evtTopicMapper.topicsFor(e)
    } yield new KeyedMessage(t, e.persistenceId, e)

    msgProducer.send(keyedMsgs: _*)
    evtProducer.send(keyedEvents: _*)

    messages.foreach(m => watermarks ! Watermarks.LastSequenceNr(m.persistenceId, m.sequenceNr))
  }

  def deleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Unit =
    deletions = deletions + (persistenceId -> (toSequenceNr, permanent))

  def deleteMessages(messageIds: Seq[PersistentId], permanent: Boolean): Unit =
    throw new UnsupportedOperationException("Individual deletions not supported")

  def writeConfirmations(confirmations: Seq[PersistentConfirmation]): Unit =
    throw new UnsupportedOperationException("Channels not supported")

  override def postStop(): Unit = {
    msgProducer.close()
    super.postStop()
  }
}

object Watermarks {
  case class LastSequenceNr(persistenceId: String, lastSequenceNr: Long)
  case class LastSequenceNrRequest(persistenceId: String)
}

class Watermarks extends Actor {
  import Watermarks._

  var lastSequenceNrs = Map.empty[String, Long]

  def receive = {
    case LastSequenceNr(persistenceId, lastSequenceNr) =>
      lastSequenceNrs = lastSequenceNrs + (persistenceId -> lastSequenceNr)
    case LastSequenceNrRequest(persistenceId) =>
      sender ! LastSequenceNr(persistenceId, lastSequenceNrs.getOrElse(persistenceId, 0L))
  }
}
