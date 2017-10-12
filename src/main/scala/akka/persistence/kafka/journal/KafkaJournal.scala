package akka.persistence.kafka.journal

import java.util.Properties

import scala.collection.immutable.Seq
import akka.persistence.{PersistentConfirmation, PersistentId}
import akka.persistence.journal.AsyncWriteJournal
import akka.serialization.{Serialization, SerializationExtension}

import scala.concurrent.Future
import scala.concurrent.duration._
import akka.actor._
import akka.pattern.ask
import akka.persistence.PersistentRepr
import akka.persistence.kafka._
import akka.persistence.kafka.MetadataConsumer.Broker
import akka.persistence.kafka.BrokerWatcher.BrokersUpdated
import akka.util.Timeout
import kafka.producer.KeyedMessage
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

class KafkaJournal extends AsyncWriteJournal with MetadataConsumer {
  import context.dispatcher

  type Deletions = Map[String, (Long, Boolean)]

  val serialization = SerializationExtension(context.system)
  val config = new KafkaJournalConfig(context.system.settings.config.getConfig("kafka-journal"))

  val brokerWatcher = new BrokerWatcher(config.zookeeperConfig, self)
  var brokers = brokerWatcher.start()

  override def postStop(): Unit = {
    brokerWatcher.stop()
    super.postStop()
  }

  override def receive: Receive = localReceive.orElse(super.receive)

  private def localReceive: Receive = {
    case BrokersUpdated(newBrokers) if newBrokers != brokers =>
      brokers = newBrokers
      journalProducerConfig = config.journalProducerConfig(brokers)
      eventProducerConfig = config.eventProducerConfig(brokers)
      writers.foreach(_ ! UpdateKafkaJournalWriterConfig(writerConfig))
  }

  // --------------------------------------------------------------------------------------
  //  Journal writes
  // --------------------------------------------------------------------------------------

  var journalProducerConfig = config.journalProducerConfig(brokers)
  var eventProducerConfig = config.eventProducerConfig(brokers)

  var writers: Vector[ActorRef] = Vector.fill(config.writeConcurrency)(writer())
  val writeTimeout = Timeout(journalProducerConfig.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG).asInstanceOf[Int].millis)

  // Transient deletions only to pass TCK (persistent not supported)
  var deletions: Deletions = Map.empty

  def asyncWriteMessages(messages: Seq[PersistentRepr]): Future[Unit] = {
    val sends = messages.groupBy(_.persistenceId).map {
      case (pid, msgs) => writerFor(pid).ask(msgs)(writeTimeout)
    }
    Future.sequence(sends).map(_ => ())
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Future[Unit] =
    Future.successful(deleteMessagesTo(persistenceId, toSequenceNr, permanent))

  def asyncDeleteMessages(messageIds: Seq[PersistentId], permanent: Boolean): Future[Unit] =
    Future.failed(new UnsupportedOperationException("Individual deletions not supported"))

  def asyncWriteConfirmations(confirmations: Seq[PersistentConfirmation]): Future[Unit] =
    Future.failed(new UnsupportedOperationException("Channels not supported"))

  def deleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Unit =
    deletions = deletions + (persistenceId -> (toSequenceNr, permanent))

  def deleteMessages(messageIds: Seq[PersistentId], permanent: Boolean): Unit =
    throw new UnsupportedOperationException("Individual deletions not supported")

  def writeConfirmations(confirmations: Seq[PersistentConfirmation]): Unit =
    throw new UnsupportedOperationException("Channels not supported")

  private def writerFor(persistenceId: String): ActorRef =
    writers(math.abs(persistenceId.hashCode) % config.writeConcurrency)

  private def writer(): ActorRef = {
    context.actorOf(Props(new KafkaJournalWriter(writerConfig)).withDispatcher(config.pluginDispatcher))
  }

  private def writerConfig = {
    KafkaJournalWriterConfig(journalProducerConfig, eventProducerConfig, config.eventTopicMapper, serialization)
  }


  // --------------------------------------------------------------------------------------
  //  Journal reads
  // --------------------------------------------------------------------------------------

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future(readHighestSequenceNr(persistenceId, fromSequenceNr))

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
    val topic = journalTopic(persistenceId)
    leaderFor(topic, brokers) match {
      case Some(Broker(host, port)) => offsetFor(host, port, topic, config.partition)
      case None                     => fromSequenceNr
    }
  }

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: PersistentRepr => Unit): Future[Unit] = {
    val deletions = this.deletions
    Future(replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max, deletions, replayCallback))
  }

  def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long, deletions: Deletions, callback: PersistentRepr => Unit): Unit = {
    val (deletedTo, permanent) = deletions.getOrElse(persistenceId, (0L, false))

    val adjustedFrom = if (permanent) math.max(deletedTo + 1L, fromSequenceNr) else fromSequenceNr
    val adjustedNum = toSequenceNr - adjustedFrom + 1L
    val adjustedTo = if (max < adjustedNum) adjustedFrom + max - 1L else toSequenceNr

    val lastSequenceNr = leaderFor(journalTopic(persistenceId), brokers) match {
      case None => 0L // topic for persistenceId doesn't exist yet
      case Some(MetadataConsumer.Broker(host, port)) =>
        val iter = persistentIterator(host, port, journalTopic(persistenceId), adjustedFrom - 1L)
        iter.map(p => if (!permanent && p.sequenceNr <= deletedTo) p.update(deleted = true) else p).foldLeft(adjustedFrom) {
          case (snr, p) => if (p.sequenceNr >= adjustedFrom && p.sequenceNr <= adjustedTo) callback(p); p.sequenceNr
        }
    }
  }

  def persistentIterator(host: String, port: Int, topic: String, offset: Long): Iterator[PersistentRepr] = {
    new MessageIterator(host, port, topic, config.partition, offset, config.consumerConfig).map { m =>
      serialization.deserialize(MessageUtil.payloadBytes(m), classOf[PersistentRepr]).get
    }
  }
}

private case class KafkaJournalWriterConfig(
  journalProducerConfig: Properties,
  eventProducerConfig: Properties,
  evtTopicMapper: EventTopicMapper,
  serialization: Serialization)

private case class UpdateKafkaJournalWriterConfig(config: KafkaJournalWriterConfig)

private class KafkaJournalWriter(var config: KafkaJournalWriterConfig) extends Actor {
  var msgProducer = createMessageProducer()
  var evtProducer = createEventProducer()

  def receive = {
    case UpdateKafkaJournalWriterConfig(newConfig) =>
      msgProducer.close()
      evtProducer.close()
      config = newConfig
      msgProducer = createMessageProducer()
      evtProducer = createEventProducer()

    case messages: Seq[PersistentRepr] =>
      writeMessages(messages)
      sender ! ()
  }

  def writeMessages(messages: Seq[PersistentRepr]): Unit = {
    val producerRecords = for {
      m <- messages
    } yield new ProducerRecord[String, Array[Byte]](journalTopic(m.persistenceId), "static", config.serialization.serialize(m).get)

    val keyedEvents = for {
      m <- messages
      e = Event(m.persistenceId, m.sequenceNr, m.payload)
      t <- config.evtTopicMapper.topicsFor(e)
    } yield new ProducerRecord[String, Array[Byte]](t, e.persistenceId, config.serialization.serialize(e).get)

    msgProducer.initTransactions()
    producerRecords.map(msgProducer.send(_))
    msgProducer.commitTransaction()

    evtProducer.initTransactions()
    producerRecords.map(evtProducer.send(_))
    evtProducer.commitTransaction()
  }

  override def postStop(): Unit = {
    msgProducer.close()
    evtProducer.close()
    super.postStop()
  }

  private def createMessageProducer() = new KafkaProducer[String, Array[Byte]](config.journalProducerConfig, new StringSerializer(), new ByteArraySerializer())

  private def createEventProducer() = new KafkaProducer[String, Array[Byte]](config.eventProducerConfig, new StringSerializer(), new ByteArraySerializer())
}
