package akka.persistence.kafka.journal

import scala.collection.immutable
import scala.util.{Failure, Success, Try}
import akka.persistence.journal.AsyncWriteJournal
import akka.serialization.SerializationExtension

import scala.concurrent.{Future, Promise}
import akka.actor._
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.persistence.kafka._
import akka.persistence.kafka.journal.KafkaJournalProtocol._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.ProducerFencedException

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

private case class SeqOfPersistentReprContainer(messages: Seq[PersistentRepr])

class KafkaJournal extends AsyncWriteJournal with MetadataConsumer with ActorLogging {
  import context.dispatcher

  type Deletions = Map[String, (Long, Boolean)]

  val serialization = SerializationExtension(context.system)
  val config = new KafkaJournalConfig(context.system.settings.config.getConfig("kafka-journal"))


  override def postStop(): Unit = {
    msgTxnProducer.close
    evtTxnProducer.close
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

  val msgTxnProducer = createMessageProducer(true)
  val evtTxnProducer = createEventProducer(true)

  // Transient deletions only to pass TCK (persistent not supported)
  var deletions: Deletions = Map.empty

  private def createMessageProducer(withTx:Boolean) = {
    val conf = if(withTx) journalProducerConfig ++ Map(ProducerConfig.TRANSACTIONAL_ID_CONFIG -> "akka-journal-message") else journalProducerConfig
    val p = new KafkaProducer[String, Array[Byte]](conf.asJava)
    if(withTx)
      p.initTransactions()
    p
  }

  private def createEventProducer(withTx:Boolean) = {
    val conf = if(withTx) eventProducerConfig ++ Map(ProducerConfig.TRANSACTIONAL_ID_CONFIG -> "akka-journal-events") else eventProducerConfig
    val p = new KafkaProducer[String, Array[Byte]](conf.asJava)
    if(withTx)
      p.initTransactions()
    p
  }

  def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {

    writeBatches(messages)
    /*if(messages.size > 1) {
      writeBatches(messages)
    } else {
      if(messages.head.payload.size > 1) {
        writeBatches(messages)
      } else {
        writeMessage(messages.head.payload.head).map { r => immutable.Seq(r) }
      }
    }*/
  }

  /*def writeMessage(message: PersistentRepr): Future[Try[Unit]] = {
    val (recordMsgs, recordEvents) = buildRecords(Seq(message))

    val fMsg = sendFuture(msgNoTxnProducer, recordMsgs.head)
    val fFinal = if(recordEvents.size > 0) {
      fMsg.map { _ =>
        sendFuture(evtNoTxnProducer, recordEvents.head)
      }
    } else {
      fMsg
    }

    fFinal.map { _ =>
      Success()
    }. recover {
      case e => e.printStackTrace();Failure(e)
    }

  }*/


  private def writeBatches(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    //Keep on sorting the messages by persistenceId, because of the transaction.
    var failure: Option[Try[Unit]] = None

    val writes = messages.groupBy(_.persistenceId).map {
      case (pid, aws) => {
        val msgs = aws.map(aw => aw.payload).flatten
        val result = writeMessages(msgs)
        if (result.isFailure)
          failure = Some(result)
      }
    }

    val promise = Promise[immutable.Seq[Try[Unit]]]()

    if (failure.isEmpty)
      promise.complete(Success(Nil)) // Happy Path
    else
      promise.failure(failure.get.failed.get)

    promise.future
  }

  private def buildRecords(messages: Seq[PersistentRepr]) = {
    val recordMsgs = for {
      m <- messages
    } yield new ProducerRecord[String, Array[Byte]](journalTopic(m.persistenceId), "static", serialization.serialize(m).get)

    val recordEvents = for {
      m <- messages
      e = Event(m.persistenceId, m.sequenceNr, m.payload)
      t <- config.eventTopicMapper.topicsFor(e)
    } yield new ProducerRecord(t, e.persistenceId, serialization.serialize(e).get)

    (recordMsgs,recordEvents)
  }

  private def writeMessages(messages: Seq[PersistentRepr]): Try[Unit] = {

    try {
      val (recordMsgs, recordEvents) = buildRecords(messages)

      msgTxnProducer.beginTransaction()
      recordMsgs.foreach { recordMsg =>
        sendFuture(msgTxnProducer, recordMsg)
      }
      msgTxnProducer.commitTransaction()

      if(recordEvents.size>0) {
        evtTxnProducer.beginTransaction()
        recordEvents.foreach { recordMsg =>
          sendFuture(evtTxnProducer, recordMsg)
        }
        evtTxnProducer.commitTransaction()
      }
      Success()
    } catch {
      case pfe:ProducerFencedException => log.error(pfe,"An error occurs");msgTxnProducer.close(); evtTxnProducer.close(); Failure(pfe)
      case ke:KafkaException => log.error(ke,"An error occurs");msgTxnProducer.abortTransaction(); evtTxnProducer.abortTransaction(); Failure(ke)
      case e:Throwable => Failure(e)
    }

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
    Math.max(offsetFor(config.offsetConsumerConfig, topic, config.partition)-1,0)
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
    new MessageIterator(config.txnAwareConsumerConfig, topic, config.partition, Math.max(offset,0)) .map { m =>
       serialization.deserialize(m.value(), classOf[PersistentRepr]).get
    }
  }
}

