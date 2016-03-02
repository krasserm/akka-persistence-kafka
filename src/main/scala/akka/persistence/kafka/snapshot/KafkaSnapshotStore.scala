package akka.persistence.kafka.snapshot

import scala.concurrent.{Promise, Future}
import scala.concurrent.duration._
import scala.util._

import akka.actor._
import akka.pattern.{ask, pipe}
import akka.persistence._
import akka.persistence.JournalProtocol._
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.kafka._
import akka.persistence.kafka.MetadataConsumer.Broker
import akka.persistence.kafka.BrokerWatcher.BrokersUpdated
import akka.serialization.SerializationExtension
import akka.util.Timeout

import akka.persistence.kafka.journal.KafkaJournalProtocol._

import _root_.kafka.producer.{Producer, KeyedMessage}

class KafkaSnapshotStore extends SnapshotStore with MetadataConsumer with ActorLogging {
  import SnapshotProtocol._
  import context.dispatcher

  val extension = Persistence(context.system)

  type RangeDeletions = Map[String, SnapshotSelectionCriteria]
  type SingleDeletions = Map[String, List[SnapshotMetadata]]

  val serialization = SerializationExtension(context.system)
  val config = new KafkaSnapshotStoreConfig(context.system.settings.config.getConfig("kafka-snapshot-store"))
  val brokerWatcher = new BrokerWatcher(config.zookeeperConfig, self)
  var brokers: List[Broker] = brokerWatcher.start()

  override def postStop(): Unit = {
    brokerWatcher.stop()
    super.postStop()
  }

  def localReceive: Receive = {
    case BrokersUpdated(newBrokers) =>
      brokers = newBrokers
  }

  override def receivePluginInternal: Receive = localReceive.orElse(super.receivePluginInternal)

  // Transient deletions only to pass TCK (persistent not supported)
  var rangeDeletions: RangeDeletions = Map.empty.withDefaultValue(SnapshotSelectionCriteria.None)
  var singleDeletions: SingleDeletions = Map.empty.withDefaultValue(Nil)

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = Future.successful {
    rangeDeletions += (persistenceId -> criteria)
  }

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = Future.successful {
    singleDeletions.get(metadata.persistenceId) match {
      case Some(dels) => singleDeletions += (metadata.persistenceId -> (metadata :: dels))
      case None       => singleDeletions += (metadata.persistenceId -> List(metadata))
    }
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = Future {
    val snapshotBytes = serialization.serialize(KafkaSnapshot(metadata, snapshot)).get
    val snapshotMessage = new KeyedMessage[String, Array[Byte]](snapshotTopic(metadata.persistenceId), "static", snapshotBytes)
    val snapshotProducer = new Producer[String, Array[Byte]](config.producerConfig(brokers))
    try {
      // TODO: take a producer from a pool
      snapshotProducer.send(snapshotMessage)
    } finally {
      snapshotProducer.close()
    }
  }

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    val singleDeletions = this.singleDeletions
    val rangeDeletions = this.rangeDeletions

    for {
      highest <- if (config.ignoreOrphan) highestJournalSequenceNr(persistenceId) else Future.successful(Long.MaxValue)
      adjusted = if (config.ignoreOrphan &&
        highest < criteria.maxSequenceNr &&
        highest > 0L) criteria.copy(maxSequenceNr = highest) else criteria
      // highest  <- Future.successful(Long.MaxValue)
      // adjusted = criteria
      snapshot <- Future {
        val topic = snapshotTopic(persistenceId)

        // if timestamp was unset on delete, matches only on same sequence nr
        def matcher(snapshot: KafkaSnapshot): Boolean = snapshot.matches(adjusted) &&
          !snapshot.matches(rangeDeletions(persistenceId)) &&
          !singleDeletions(persistenceId).contains(snapshot.metadata) &&
          !singleDeletions(persistenceId).filter(_.timestamp == 0L).map(_.sequenceNr).contains(snapshot.metadata.sequenceNr)

        leaderFor(topic, brokers) match {
          case Some(Broker(host, port)) => load(host, port, topic, matcher).map(s => SelectedSnapshot(s.metadata, s.snapshot))
          case None => None
        }
      }
    } yield snapshot
  }

  def load(host: String, port: Int, topic: String, matcher: KafkaSnapshot => Boolean): Option[KafkaSnapshot] = {
    val offset = offsetFor(host, port, topic, config.partition)

    @annotation.tailrec
    def load(host: String, port: Int, topic: String, offset: Long): Option[KafkaSnapshot] =
      if (offset < 0) None else {
        val s = snapshot(host, port, topic, offset)
        if (matcher(s)) Some(s) else load(host, port, topic, offset - 1)
      }

    load(host, port, topic, offset - 1)
  }

  /**
   * Fetches the highest sequence number for `persistenceId` from the journal actor.
    */

  private def highestJournalSequenceNr(persistenceId: String): Future[Long] = {
    val journal = extension.journalFor(null);
    implicit val timeout = Timeout(5 seconds)
    val res = journal ? ReadHighestSequenceNr(0L, persistenceId, self)
    res.flatMap {
      case ReadHighestSequenceNrSuccess(snr) => Future.successful(snr)
      case ReadHighestSequenceNrFailure(err) => Future.failed(err)
    }
  }

  private def snapshot(host: String, port: Int, topic: String, offset: Long): KafkaSnapshot = {
    val iter = new MessageIterator(host, port, topic, config.partition, offset, config.consumerConfig)
    try { serialization.deserialize(MessageUtil.payloadBytes(iter.next()), classOf[KafkaSnapshot]).get } finally { iter.close() }
  }

  private def snapshotTopic(persistenceId: String): String =
    s"${config.prefix}${journalTopic(persistenceId)}"
}

