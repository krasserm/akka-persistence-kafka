package akka.persistence.kafka.journal

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask
import akka.persistence.PersistentRepr
import akka.util.Timeout

import kafka.api._
import kafka.common.LeaderNotAvailableException
import kafka.consumer._
import kafka.message._

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
        val iter = new PersistentReprIterator(host, port, persistenceId, 0, adjustedFrom - 1L)
        iter.map(p => if (!permanent && p.sequenceNr <= deletedTo) p.update(deleted = true) else p).foldLeft(0L) {
          case (snr, p) => if (p.sequenceNr >= adjustedFrom && p.sequenceNr <= adjustedTo) callback(p); p.sequenceNr
        }
    }

    watermarks ! LastSequenceNr(persistenceId, lastSequenceNr)
  }

  private class PersistentReprIterator(host: String, port: Int, topic: String, partition: Int, offset: Long) extends Iterator[PersistentRepr] {
    val iterator = new MessageIterator(host, port, topic, partition, offset)

    override def next(): PersistentRepr = {
      val m = iterator.next()
      val payload = m.payload
      val payloadBytes = Array.ofDim[Byte](payload.limit())

      payload.get(payloadBytes)
      serialization.deserialize(payloadBytes, classOf[PersistentRepr]).get
    }

    override def hasNext: Boolean =
      iterator.hasNext
  }

  private class MessageIterator(host: String, port: Int, topic: String, partition: Int, offset: Long) extends Iterator[Message] {
    import config.journalConsumerConfig._

    val consumer = new SimpleConsumer(host, port, socketTimeoutMs, socketReceiveBufferBytes, clientId)
    var iter = iterator(offset)
    var read = 0
    var nxto = offset

    def iterator(offset: Long): Iterator[MessageAndOffset] = {
      val request = new FetchRequestBuilder().addFetch(topic, partition, offset, fetchMessageMaxBytes).build()
      val response = consumer.fetch(request)
      response.messageSet(topic, partition).iterator
    }

    def next(): Message = {
      val mo = iter.next()
      read += 1
      nxto = mo.nextOffset
      mo.message
    }

    @annotation.tailrec
    final def hasNext: Boolean =
      if (iter.hasNext) {
        true
      } else if (read == 0) {
        close()
        false
      } else {
        iter = iterator(nxto)
        read = 0
        hasNext
      }

    def close(): Unit = {
      consumer.close()
    }
  }
}
