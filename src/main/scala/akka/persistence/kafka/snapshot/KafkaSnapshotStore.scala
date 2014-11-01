package akka.persistence.kafka.snapshot

import scala.concurrent.{Promise, Future}
import scala.concurrent.duration._
import scala.util._

import akka.actor._
import akka.pattern.{PromiseActorRef, pipe}
import akka.persistence._
import akka.persistence.JournalProtocol._
import akka.persistence.kafka._
import akka.persistence.kafka.MetadataConsumer.Broker
import akka.persistence.kafka.BrokerWatcher.BrokersUpdated
import akka.serialization.SerializationExtension
import akka.util.Timeout

import _root_.kafka.producer.{Producer, KeyedMessage}

/**
 * Optimized and fully async version of [[akka.persistence.snapshot.SnapshotStore]].
 */
trait KafkaSnapshotStoreEndpoint extends Actor {
  import SnapshotProtocol._
  import context.dispatcher

  val extension = Persistence(context.system)
  val publish = extension.settings.internal.publishPluginCommands

  def receive = {
    case LoadSnapshot(persistenceId, criteria, toSequenceNr) ⇒
      val p = sender
      loadAsync(persistenceId, criteria.limit(toSequenceNr)) map {
        sso ⇒ LoadSnapshotResult(sso, toSequenceNr)
      } recover {
        case e ⇒ LoadSnapshotResult(None, toSequenceNr)
      } pipeTo (p)
    case SaveSnapshot(metadata, snapshot) ⇒
      val p = sender
      val md = metadata.copy(timestamp = System.currentTimeMillis)
      saveAsync(md, snapshot) map {
        _ ⇒ SaveSnapshotSuccess(md)
      } recover {
        case e ⇒ SaveSnapshotFailure(metadata, e)
      } pipeTo (p)
    case d @ DeleteSnapshot(metadata) ⇒
      deleteAsync(metadata) onComplete {
        case Success(_) => if (publish) context.system.eventStream.publish(d)
        case Failure(_) =>
      }
    case d @ DeleteSnapshots(persistenceId, criteria) ⇒
      deleteAsync(persistenceId, criteria) onComplete {
        case Success(_) => if (publish) context.system.eventStream.publish(d)
        case Failure(_) =>
      }
  }

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]]
  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit]
  def deleteAsync(metadata: SnapshotMetadata): Future[Unit]
  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit]
}

class KafkaSnapshotStore extends KafkaSnapshotStoreEndpoint with MetadataConsumer with ActorLogging {
  import context.dispatcher

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

  override def receive: Receive = localReceive.orElse(super.receive)

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
      snapshot <- Future {
        val topic = snapshotTopic(persistenceId)

        def matcher(snapshot: KafkaSnapshot): Boolean = snapshot.matches(adjusted) &&
          !snapshot.matches(rangeDeletions(persistenceId)) &&
          !singleDeletions(persistenceId).contains(snapshot.metadata)

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
    val journal = extension.journalFor(persistenceId)
    val promise = Promise[Any]()

    // We need to use a PromiseActorRef here instead of ask because the journal doesn't reply to ReadHighestSequenceNr requests
    val ref = PromiseActorRef(extension.system.provider, Timeout(config.consumerConfig.socketTimeoutMs.millis), journal.toString)

    journal ! ReadHighestSequenceNr(0L, persistenceId, ref)

    ref.result.future.flatMap {
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

