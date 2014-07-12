package akka.persistence.kafka.journal

import akka.actor._
import akka.persistence.PersistentActor
import akka.persistence.kafka._
import akka.persistence.kafka.server.TestServer
import akka.testkit._

import com.typesafe.config.ConfigFactory

import kafka.consumer.Consumer

import org.scalatest._
import kafka.serializer.StringDecoder

object KafkaEventSpec {
  val config = ConfigFactory.parseString(
    """
      |akka.persistence.snapshot-store.local.dir = "target/snapshots"
      |akka.persistence.journal.plugin = "kafka-journal"
      |akka.test.single-expect-default = 10s
      |kafka-journal.consumer.consumer.timeout.ms = 10000
      |kafka-journal.consumer.auto.commit.enable = false
      |kafka-journal.consumer.auto.offset.reset = "smallest"
      |kafka-journal.zookeeper.connection.timeout.ms = 10000
      |kafka-journal.zookeeper.session.timeout.ms = 10000
    """.stripMargin)

  class TestPersistentActor(val persistenceId: String) extends PersistentActor {
    def receiveCommand = { case s: String => persist(s)(_ => ()) }
    def receiveRecover = { case s: String => }
  }
}

class KafkaEventSpec extends TestKit(ActorSystem("test", KafkaEventSpec.config)) with ImplicitSender with WordSpecLike with Matchers with KafkaCleanup {
  import KafkaEventSpec._

  val server = new TestServer()
  val config = new KafkaJournalConfig(system.settings.config.getConfig("kafka-journal"))

  akka.persistence.Persistence(system).journalFor(null)

  val pa = system.actorOf(Props(new TestPersistentActor("pa")))
  val pb = system.actorOf(Props(new TestPersistentActor("pb")))
  val pc = system.actorOf(Props(new TestPersistentActor("pc")))


  override def afterAll(): Unit = {
    server.stop()
    system.shutdown()
    super.afterAll()
  }

  "A Kafka Journal" must {
    "publish all events to the events topic by default" in {
      val consConn = Consumer.create(config.journalConsumerConfig)
      val streams = consConn.createMessageStreams(Map("events" -> 1),
        keyDecoder = new StringDecoder,
        valueDecoder = new DefaultEventDecoder())

      val stream = streams("events")(0)

      1 to 3 foreach { i =>
        pa ! s"a-${i}"
        pb ! s"b-${i}"
        pc ! s"c-${i}"
      }

      val events = stream.take(9).map(_.message).toList.groupBy(_.persistenceId)

      events("pa") should be(List(Event("pa", 1L, "a-1"), Event("pa", 2L, "a-2"), Event("pa", 3L, "a-3")))
      events("pb") should be(List(Event("pb", 1L, "b-1"), Event("pb", 2L, "b-2"), Event("pb", 3L, "b-3")))
      events("pc") should be(List(Event("pc", 1L, "c-1"), Event("pc", 2L, "c-2"), Event("pc", 3L, "c-3")))
    }
  }
}
