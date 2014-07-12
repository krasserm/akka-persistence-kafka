package akka.persistence.kafka.journal

import scala.collection.immutable.Seq

import akka.actor._
import akka.persistence.{PersistentId, PersistentConfirmation, PersistentRepr}
import akka.persistence.journal.SyncWriteJournal
import akka.persistence.kafka._
import akka.serialization.SerializationExtension

import kafka.producer._

class KafkaJournal extends SyncWriteJournal with KafkaMetadata with KafkaRecovery {
  val serialization = SerializationExtension(context.system)

  val msgProducer = new Producer[String, Array[Byte]](config.journalProducerConfig(brokers.map(_.toString)))
  val evtProducer = new Producer[String, Event](config.eventProducerConfig(brokers.map(_.toString)))
  val evtTopicMapper = config.eventTopicMapper

  val watermarks: ActorRef = context.actorOf(Props[Watermarks])
  var deletions = Map.empty[String, (Long, Boolean)] // TODO: make persistent

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

    // TODO: this can be optimized by sending only the highest sequenceNr per persistenceId
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
    evtProducer.close()
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
