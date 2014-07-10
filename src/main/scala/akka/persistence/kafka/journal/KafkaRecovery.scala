package akka.persistence.kafka.journal

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util._

import akka.actor._
import akka.pattern.ask
import akka.persistence.PersistentRepr
import akka.util.Timeout

import kafka.api._
import kafka.consumer._
import kafka.message._

trait KafkaRecovery { this: KafkaJournal =>
  import Watermarks._

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(config.getString("replay-dispatcher"))

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    watermarks.ask(LastSequenceNrRequest(persistenceId))(Timeout(10.seconds)).map {
      case LastSequenceNr(_, lastSequenceNr) => math.max(lastSequenceNr, fromSequenceNr)
    }

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: PersistentRepr => Unit): Future[Unit] =
    Future(replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max, replayCallback))(replayDispatcher)

  def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long, callback: PersistentRepr => Unit): Unit = {
    val (deletedTo, permanent) = deletions.getOrElse(persistenceId, (0L, false))

    val from = if (permanent) math.max(deletedTo + 1L, fromSequenceNr) else fromSequenceNr
    val num = toSequenceNr - from + 1L
    val to = if (max < num) from + max - 1L else toSequenceNr

    def replayOnce: Long = {
      // --------------------------------------------------------------------------------------------
      // TODO: verify that partitions with id = 0 of different topics are distributed across brokers
      // --------------------------------------------------------------------------------------------
      new KafkaPersistentIterator(leaderFor(persistenceId), 6667, persistenceId, 0, from - 1L).map { p =>
        if (!permanent && p.sequenceNr <= deletedTo) p.update(deleted = true) else p
      }.foldLeft(0L) {
        case (snr, p) => if (p.sequenceNr >= from && p.sequenceNr <= to) callback(p); p.sequenceNr
      }
    }

    def replay(retry: Int): Unit = Try(replayOnce) match {
      case Success(snr) => watermarks ! LastSequenceNr(persistenceId, snr)
      case Failure(thr) => if (retry < 3) replay(retry + 1) else throw thr
    }

    replay(0)
  }

  private def leaderFor(persistenceId: String, seed: String = "localhost"): String = {
    import TopicMetadataRequest._

    val consumer = new SimpleConsumer(seed, 6667, 100000, 64 * 1024, "replayer")
    val request = new TopicMetadataRequest(CurrentVersion, 0, DefaultClientId, List(persistenceId))
    val response = consumer.send(request)

    val brokers = for {
      tmd    <- response.topicsMetadata
      pmd    <- tmd.partitionsMetadata
      broker <- pmd.leader
    } yield broker.host

    brokers.headOption.getOrElse(throw new Exception(s"no leader for topic ${persistenceId}"))
  }

  private class KafkaPersistentIterator(host: String, port: Int, topic: String, partition: Int, offset: Long) extends Iterator[PersistentRepr] {
    val iterator = new KafkaMessageIterator(host, port, topic, partition, offset)

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

  private class KafkaMessageIterator(host: String, port: Int, topic: String, partition: Int, offset: Long) extends Iterator[Message] {
    val consumer = new SimpleConsumer(host, port, 100000, 64 * 1024, "replayer")
    var iter = iterator(offset)
    var read = 0
    var nxto = offset

    def iterator(offset: Long): Iterator[MessageAndOffset] = {
      val request = new FetchRequestBuilder().addFetch(topic, partition, offset, 100000).build()
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
