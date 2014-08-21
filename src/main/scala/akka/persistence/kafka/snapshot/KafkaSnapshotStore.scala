package akka.persistence.kafka.snapshot

import scala.concurrent.Future
import scala.util._

import akka.actor._
import akka.pattern.pipe
import akka.persistence._
import akka.persistence.kafka._
import akka.persistence.kafka.MetadataConsumer.Broker
import akka.serialization.SerializationExtension

import _root_.kafka.producer.{Producer, KeyedMessage}

/**
 * Optimized and fully async version of [[akka.persistence.snapshot.SnapshotStore]].
 */
trait KafkaSnapshotStoreEndpoint extends Actor {
  import SnapshotProtocol._
  import context.dispatcher

  val extension = Persistence(context.system)
  val publish = extension.settings.internal.publishPluginCommands

  final def receive = {
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

  val serialization = SerializationExtension(context.system)
  val config = new KafkaSnapshotStoreConfig(context.system.settings.config.getConfig("kafka-snapshot-store"))
  val brokers = allBrokers()

  // Transient deletions only to pass TCK (persistent not supported)
  var rangeDeletions = Map.empty[String, SnapshotSelectionCriteria].withDefaultValue(SnapshotSelectionCriteria.None)
  var singleDeletions = Map.empty[String, List[SnapshotMetadata]].withDefaultValue(Nil)

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
    snapshotProducer.send(snapshotMessage)
  }

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = Future {
    val topic = snapshotTopic(persistenceId)
    leaderFor(topic, brokers) match {
      case None => None
      case Some(Broker(host, port)) => {
        import SnapshotSelectionCriteria.Latest

        val offset = criteria match {
          case Latest => offsetFor(host, port, topic, config.partition)
          case _      => 1
        }

        val iter = snapshotIterator(host, port, topic, offset - 1) filter { sm =>
          sm.matches(criteria) && !sm.matches(rangeDeletions(persistenceId)) && !singleDeletions(persistenceId).contains(sm.metadata)
        }

        // -----------------------------------------------------------------------------------
        // This can have terrible performance if criteria != SnapshotSelectionCriteria.Latest
        // as it iterates over stored all snapshots. Memory consumption is however limited by
        // fetch.message.max.bytes consumer property.
        // -----------------------------------------------------------------------------------
        iter.foldLeft[Option[SelectedSnapshot]](None) {
          case (r, KafkaSnapshot(metadata, snapshot)) => Some(SelectedSnapshot(metadata, snapshot))
        }
      }
    }
  }

  private def snapshotIterator(host: String, port: Int, topic: String, offset: Long): Iterator[KafkaSnapshot] =
    new MessageIterator(host, port, topic, config.partition, offset, config.consumerConfig).map { m =>
      serialization.deserialize(MessageUtil.payloadBytes(m), classOf[KafkaSnapshot]).get
    }

  private def snapshotTopic(persistenceId: String): String =
    s"${config.prefix}${topic(persistenceId)}"
}

