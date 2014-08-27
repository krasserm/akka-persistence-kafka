package akka.persistence.kafka.journal

import scala.collection.immutable.Seq

import akka.actor._
import akka.persistence._
import akka.persistence.kafka._
import akka.persistence.kafka.server._
import akka.serialization.SerializationExtension
import akka.testkit._

import com.typesafe.config.ConfigFactory

import _root_.kafka.message.Message

import org.scalatest._

object KafkaIntegrationSpec {
  val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "kafka-journal"
      |akka.persistence.snapshot-store.plugin = "kafka-snapshot-store"
      |akka.test.single-expect-default = 10s
      |kafka-journal.event.producer.request.required.acks = 1
      |kafka-journal.zookeeper.connection.timeout.ms = 10000
      |kafka-journal.zookeeper.session.timeout.ms = 10000
      |test-server.zookeeper.dir = target/test/zookeeper
      |test-server.kafka.log.dirs = target/test/kafka
    """.stripMargin)

  class TestPersistentActor(val persistenceId: String) extends PersistentActor {
    def receiveCommand = { case s: String => persist(s)(s => sender ! s) }
    def receiveRecover = { case s: String => }
  }
}

class KafkaIntegrationSpec extends TestKit(ActorSystem("test", KafkaIntegrationSpec.config)) with ImplicitSender with WordSpecLike with Matchers with KafkaCleanup {
  import KafkaIntegrationSpec._
  import MessageUtil._

  val systemConfig = system.settings.config
  val journalConfig = new KafkaJournalConfig(systemConfig.getConfig("kafka-journal"))
  val serverConfig = new TestServerConfig(systemConfig.getConfig("test-server"))
  val server = new TestServer(serverConfig)

  val serialization = SerializationExtension(system)
  val eventDecoder = new DefaultEventDecoder

  val pa = system.actorOf(Props(new TestPersistentActor("pa")))
  val pb = system.actorOf(Props(new TestPersistentActor("pb")))
  val pc = system.actorOf(Props(new TestPersistentActor("pc")))

  override def beforeAll(): Unit = {
    super.beforeAll()
    1 to 3 foreach { i =>
      pa ! s"a-${i}"; expectMsg(s"a-${i}")
      pb ! s"b-${i}"; expectMsg(s"b-${i}")
      pc ! s"c-${i}"; expectMsg(s"c-${i}")
    }
  }

  override def afterAll(): Unit = {
    server.stop()
    system.shutdown()
    super.afterAll()
  }

  import serverConfig._

  def persistent(topic: String): Seq[PersistentRepr] =
    messages(topic, 0).map(m => serialization.deserialize(payloadBytes(m), classOf[PersistentRepr]).get)

  def events(partition: Int): Seq[Event] =
    messages("events", partition).map(m => eventDecoder.fromBytes(payloadBytes(m)))

  def messages(topic: String, partition: Int): Seq[Message] =
    new MessageIterator(kafka.hostName, kafka.port, topic, partition, 0, journalConfig.consumerConfig).toVector

  "A Kafka Journal" must {
    "publish all events to the events topic by default" in {
      val eventSeq = for {
        partition <- 0 until kafka.numPartitions
        event <- events(partition)
      } yield event

      val eventMap = eventSeq.groupBy(_.persistenceId)

      eventMap("pa") should be(Seq(Event("pa", 1L, "a-1"), Event("pa", 2L, "a-2"), Event("pa", 3L, "a-3")))
      eventMap("pb") should be(Seq(Event("pb", 1L, "b-1"), Event("pb", 2L, "b-2"), Event("pb", 3L, "b-3")))
      eventMap("pc") should be(Seq(Event("pc", 1L, "c-1"), Event("pc", 2L, "c-2"), Event("pc", 3L, "c-3")))
    }
    "publish events for each persistent actor to a separate topic" in {
      persistent("pa").map(_.payload) should be(Seq("a-1", "a-2", "a-3"))
      persistent("pb").map(_.payload) should be(Seq("b-1", "b-2", "b-3"))
      persistent("pc").map(_.payload) should be(Seq("c-1", "c-2", "c-3"))
    }
    "properly encode topic names" in {
      val actorId = "a/b/c" // not a valid topic name
      val actor = system.actorOf(Props(new TestPersistentActor(actorId)))

      actor ! "a"; expectMsg("a")
      actor ! "b"; expectMsg("b")

      persistent(journalTopic(actorId)).map(_.payload) should be(Seq("a", "b"))
    }
  }
}
