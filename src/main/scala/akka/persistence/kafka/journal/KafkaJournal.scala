package akka.persistence.kafka.journal

import scala.collection.immutable
import scala.util.{Failure, Success, Try}
import akka.persistence.journal.AsyncWriteJournal
import akka.serialization.{Serialization, SerializationExtension}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import akka.actor._
import akka.pattern.ask
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.persistence.kafka._
import akka.persistence.kafka.journal.KafkaJournalProtocol._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.ProducerFencedException
import akka.util.Timeout

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

private case class SeqOfPersistentReprContainer(messages: Seq[PersistentRepr])
private case object CloseWriter

class KafkaJournal extends AsyncWriteJournal with MetadataConsumer with ActorLogging {
  import context.dispatcher

  type Deletions = Map[String, (Long, Boolean)]

  val serialization = SerializationExtension(context.system)
  val config = new KafkaJournalConfig(context.system.settings.config.getConfig("kafka-journal"))


  override def postStop(): Unit = {
    writers.foreach { writer =>
      writer ! CloseWriter
      writer ! PoisonPill
    }
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

  // Transient deletions only to pass TCK (persistent not supported)
  var deletions: Deletions = Map.empty

  val writers: Vector[ActorRef] = Vector.tabulate(config.writeConcurrency)(i => writer(i))

  val writeTimeout = Timeout(config.writerTimeoutMs.millis)

  def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    val writes = messages.groupBy(msg => msg.persistenceId).map {
      case (pid,aws) => writerFor(pid).ask(SeqOfPersistentReprContainer(aws.map{aw=>aw.payload}.flatten))(writeTimeout).mapTo[Try[Unit]]
    }

    Future.sequence(writes.to[immutable.Seq]).map { _ =>
      Nil //Happy path!!!
    }.recover {
      case e:Throwable => immutable.Seq.fill(messages.length)(Failure(e))
    }
  }

  private def writerFor(persistenceId: String): ActorRef =
    writers(math.abs(persistenceId.hashCode) % config.writeConcurrency)

  private def writer(index:Int): ActorRef = {
    context.actorOf(Props(new KafkaJournalWriter(index,config,serialization)).withDispatcher(config.pluginDispatcher))
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
    Math.max(offsetFor(config.txnAwareConsumerConfig, topic, config.partition)-1,0)
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


private class KafkaJournalWriter(index:Int,config: KafkaJournalConfig,serialization:Serialization) extends Actor with ActorLogging {
  var msgProducer = createMessageProducer(index)
  var evtProducer = createEventProducer(index)

  def receive = {
    case messages: SeqOfPersistentReprContainer =>
      val result = writeMessages(messages.messages)
      sender() ! result
    case CloseWriter =>
      msgProducer.close()
      evtProducer.close()
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

      msgProducer.beginTransaction()
      recordMsgs.foreach { recordMsg =>
        sendFuture(msgProducer, recordMsg)
      }
      msgProducer.commitTransaction()

      if(recordEvents.size>0) {
        evtProducer.beginTransaction()
        recordEvents.foreach { recordMsg =>
          sendFuture(evtProducer, recordMsg)
        }
        evtProducer.commitTransaction()
      }
      Success()
    } catch {
      case pfe:ProducerFencedException => log.error(pfe,"An error occurs")
        msgProducer.close()
        evtProducer.close()
        msgProducer = createMessageProducer(index)
        evtProducer = createEventProducer(index)
        Failure(pfe)
      case ke:KafkaException => log.error(ke,"An error occurs")
        msgProducer.abortTransaction()
        evtProducer.abortTransaction()
        Failure(ke)
      case e:Throwable => log.error(e,"An error occurs");Failure(e)
    }

  }

  override def postStop(): Unit = {
    msgProducer.close()
    evtProducer.close()
    super.postStop()
  }

  private def createMessageProducer(index:Int) = {
    val conf = config.journalProducerConfig() ++ Map(ProducerConfig.TRANSACTIONAL_ID_CONFIG -> s"akka-journal-message-${context.system.name}-$index")
    val p = new KafkaProducer[String, Array[Byte]](conf.asJava)
    p.initTransactions()
    p
  }

  private def createEventProducer(index:Int) = {
    val conf = config.eventProducerConfig() ++ Map(ProducerConfig.TRANSACTIONAL_ID_CONFIG -> s"akka-journal-events-${context.system.name}-$index")
    val p = new KafkaProducer[String, Array[Byte]](conf.asJava)
    p.initTransactions()
    p
  }
}

