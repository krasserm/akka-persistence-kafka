package akka.persistence.kafka.journal

import scala.collection.immutable
import scala.util.{Failure, Success, Try}
import akka.persistence.journal.AsyncWriteJournal
import akka.serialization.SerializationExtension

import scala.concurrent.Future
import akka.actor._
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.persistence.kafka._
import akka.persistence.kafka.journal.KafkaJournalProtocol._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConverters._

private case class SeqOfPersistentReprContainer(messages: Seq[PersistentRepr])

class KafkaJournal extends AsyncWriteJournal with MetadataConsumer with ActorLogging {
  import context.dispatcher

  type Deletions = Map[String, (Long, Boolean)]

  val serialization = SerializationExtension(context.system)
  val config = new KafkaJournalConfig(context.system.settings.config.getConfig("kafka-journal"))


  override def postStop(): Unit = {
    super.postStop()
  }

  override def receivePluginInternal: Receive = localReceive.orElse(super.receivePluginInternal)

  private def localReceive: Receive = {
    case ReadHighestSequenceNr(fromSequenceNr, persistenceId, persistentActor) =>
      try {
        val highest = readHighestSequenceNr(persistenceId, fromSequenceNr)
        sender ! ReadHighestSequenceNrSuccess(highest)
      } catch {
        case e : Exception => sender ! ReadHighestSequenceNrFailure(e)
      }
  }

  // --------------------------------------------------------------------------------------
  //  Journal writes
  // --------------------------------------------------------------------------------------

  val journalProducerConfig = config.journalProducerConfig()
  val eventProducerConfig = config.eventProducerConfig()

  val msgProducer = createMessageProducer()
  val evtProducer = createEventProducer()

  // Transient deletions only to pass TCK (persistent not supported)
  var deletions: Deletions = Map.empty

  private def createMessageProducer() = {
    val p = new KafkaProducer[String, Array[Byte]](journalProducerConfig.asJava)
    p.initTransactions()
    p
  }

  private def createEventProducer() = {
    val p = new KafkaProducer[String, Array[Byte]](eventProducerConfig.asJava)
    p.initTransactions()
    p
  }

  def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    //Keep on sorting the messages by persistenceId, because of the transaction.
    val writes = Future.sequence(messages.groupBy(_.persistenceId).map {
      case (pid,aws) => {
        val msgs = aws.map(aw => aw.payload).flatten
        writeMessages(msgs)
      }
    }).map { x => x.flatten.to[collection.immutable.Seq] }
    
    writes
  }

  private def writeMessages(messages: Seq[PersistentRepr]): Future[Seq[Try[Unit]]] = {
    val recordMsgs = for {
      m <- messages
    } yield new ProducerRecord[String, Array[Byte]](journalTopic(m.persistenceId), "static", serialization.serialize(m).get)

    val recordEvents = for {
      m <- messages
      e = Event(m.persistenceId, m.sequenceNr, m.payload)
      t <- config.eventTopicMapper.topicsFor(e)
    } yield new ProducerRecord(t, e.persistenceId, serialization.serialize(e).get)

    msgProducer.beginTransaction()
    val fRecords = recordMsgs.map { recordMsg =>
      sendFuture(msgProducer,recordMsg).map {_ => Success()}
    }
    val retMsgs = Future.sequence(fRecords).andThen {
      case Success(_) => msgProducer.commitTransaction()
      case Failure(e) => msgProducer.abortTransaction()
    }

    evtProducer.beginTransaction()
    val fEvents = recordEvents.map { recordMsg =>
      sendFuture(evtProducer,recordMsg).map {_ => Success()}
    }
    val retEvents = Future.sequence(fEvents).andThen {
      case Success(_) =>  evtProducer.commitTransaction()
      case Failure(e) => evtProducer.abortTransaction()
    }

    retMsgs.flatMap { _ => retEvents}
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    Future.successful(deleteMessagesTo(persistenceId, toSequenceNr, false))

  def deleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Unit =
    deletions = deletions + (persistenceId -> (toSequenceNr, permanent))

  // --------------------------------------------------------------------------------------
  //  Journal reads
  // --------------------------------------------------------------------------------------

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future(readHighestSequenceNr(persistenceId, fromSequenceNr))

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
    val topic = journalTopic(persistenceId)
    Math.max(offsetFor(config.journalConsumerConfig, topic, config.partition)-1,0)
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

    val lastSequenceNr = {
      val iter = persistentIterator(journalTopic(persistenceId), adjustedFrom - 1L)
      iter.map(p => if (!permanent && p.sequenceNr <= deletedTo) p.update(deleted = true) else p).foldLeft(adjustedFrom) {
        case (snr, p) => if (p.sequenceNr >= adjustedFrom && p.sequenceNr <= adjustedTo) callback(p); p.sequenceNr
      }
    }

  }

  def persistentIterator(topic: String, offset: Long): Iterator[PersistentRepr] = {
    val adjustedOffset = if(offset < 0) 0 else offset
    new MessageIterator(config.journalConsumerConfig, topic, config.partition, adjustedOffset).map { m =>
      serialization.deserialize(m.value(), classOf[PersistentRepr]).get
    }
  }
}

