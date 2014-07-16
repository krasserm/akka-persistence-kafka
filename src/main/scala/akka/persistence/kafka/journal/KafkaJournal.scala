package akka.persistence.kafka.journal

import scala.collection.immutable.Seq

import akka.persistence.{PersistentId, PersistentConfirmation}
import akka.persistence.journal.SyncWriteJournal
import akka.serialization.SerializationExtension

import kafka.producer._

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask
import akka.persistence.PersistentRepr
import akka.persistence.kafka._
import akka.util.Timeout


class KafkaJournal extends SyncWriteJournal with MetadataConsumer {
  import Watermarks._

  val serialization = SerializationExtension(context.system)
  val config = new KafkaJournalConfig(context.system.settings.config.getConfig("kafka-journal"))
  val brokers = allBrokers()

  val watermarks: ActorRef = context.actorOf(Props[Watermarks])
  // Transient deletions only to pass TCK (persistent not supported)
  var deletions = Map.empty[String, (Long, Boolean)]

  // --------------------------------------------------------------------------------------
  //  Journal writes
  // --------------------------------------------------------------------------------------

  val msgProducer = new Producer[String, Array[Byte]](config.journalProducerConfig(brokers))
  val evtProducer = new Producer[String, Event](config.eventProducerConfig(brokers))
  val evtTopicMapper = config.eventTopicMapper

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

  // --------------------------------------------------------------------------------------
  //  Journal reads
  // --------------------------------------------------------------------------------------

  implicit val replayDispatcher = context.system.dispatchers.lookup(config.replayDispatcher)

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    watermarks.ask(LastSequenceNrRequest(persistenceId))(Timeout(10.seconds)).map {
      case LastSequenceNr(_, lastSequenceNr) => math.max(lastSequenceNr, fromSequenceNr)
    }

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: PersistentRepr => Unit): Future[Unit] =
    Future(replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max, replayCallback))(replayDispatcher)

  def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long, callback: PersistentRepr => Unit): Unit = {
    val (deletedTo, permanent) = deletions.getOrElse(persistenceId, (0L, false))

    val adjustedFrom = if (permanent) math.max(deletedTo + 1L, fromSequenceNr) else fromSequenceNr
    val adjustedNum = toSequenceNr - adjustedFrom + 1L
    val adjustedTo = if (max < adjustedNum) adjustedFrom + max - 1L else toSequenceNr

    val lastSequenceNr = leaderFor(persistenceId, brokers) match {
      case None => 0L // topic for persistenceId doesn't exist yet
      case Some(MetadataConsumer.Broker(host, port)) =>
        val iter = persistentIterator(host, port, persistenceId, adjustedFrom - 1L)
        iter.map(p => if (!permanent && p.sequenceNr <= deletedTo) p.update(deleted = true) else p).foldLeft(0L) {
          case (snr, p) => if (p.sequenceNr >= adjustedFrom && p.sequenceNr <= adjustedTo) callback(p); p.sequenceNr
        }
    }

    watermarks ! LastSequenceNr(persistenceId, lastSequenceNr)
  }

  def persistentIterator(host: String, port: Int, topic: String, offset: Long): Iterator[PersistentRepr] = {
    new MessageIterator(host, port, topic, config.partition, offset, config.consumerConfig).map { m =>
      serialization.deserialize(MessageUtil.payloadBytes(m), classOf[PersistentRepr]).get
    }
  }

  override def postStop(): Unit = {
    msgProducer.close()
    evtProducer.close()
    super.postStop()
  }
}

private object Watermarks {
  case class LastSequenceNr(persistenceId: String, lastSequenceNr: Long)
  case class LastSequenceNrRequest(persistenceId: String)
}

private class Watermarks extends Actor {
  import Watermarks._

  var lastSequenceNrs = Map.empty[String, Long]

  def receive = {
    case LastSequenceNr(persistenceId, lastSequenceNr) =>
      lastSequenceNrs = lastSequenceNrs + (persistenceId -> lastSequenceNr)
    case LastSequenceNrRequest(persistenceId) =>
      sender ! LastSequenceNr(persistenceId, lastSequenceNrs.getOrElse(persistenceId, 0L))
  }
}
