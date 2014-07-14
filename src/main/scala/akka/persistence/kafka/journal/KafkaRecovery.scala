package akka.persistence.kafka.journal

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask
import akka.persistence.PersistentRepr
import akka.util.Timeout

trait KafkaRecovery extends KafkaMetadata { this: KafkaJournal =>
  import KafkaMetadata._
  import Watermarks._

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
      case Some(Broker(host, port)) =>
        val iter = persistentIterator(host, port, persistenceId, adjustedFrom - 1L)
        iter.map(p => if (!permanent && p.sequenceNr <= deletedTo) p.update(deleted = true) else p).foldLeft(0L) {
          case (snr, p) => if (p.sequenceNr >= adjustedFrom && p.sequenceNr <= adjustedTo) callback(p); p.sequenceNr
        }
    }

    watermarks ! LastSequenceNr(persistenceId, lastSequenceNr)
  }

  def persistentIterator(host: String, port: Int, topic: String, offset: Long): Iterator[PersistentRepr] = {
    new KafkaMessageIterator(host, port, topic, 0, offset, config.journalConsumerConfig).map { m =>
      serialization.deserialize(KafkaMessage.payloadBytes(m), classOf[PersistentRepr]).get
    }
  }
}
