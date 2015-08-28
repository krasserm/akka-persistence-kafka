package akka.persistence.kafka.journal

import scala.concurrent.duration._

import akka.actor._
import akka.persistence.PersistentActor
import akka.persistence.kafka._
import akka.persistence.kafka.server._
import akka.testkit._

import com.typesafe.config.ConfigFactory

import org.scalatest._

object KafkaLoadSpec {
  val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "kafka-journal"
      |akka.persistence.snapshot-store.plugin = "kafka-snapshot-store"
      |akka.test.single-expect-default = 10s
      |kafka-journal.event.producer.request.required.acks = 1
      |kafka-journal.event.producer.topic.mapper.class = "akka.persistence.kafka.EmptyEventTopicMapper"
      |kafka-journal.zookeeper.connection.timeout.ms = 10000
      |kafka-journal.zookeeper.session.timeout.ms = 10000
      |test-server.zookeeper.dir = target/test/zookeeper
      |test-server.kafka.log.dirs = target/test/kafka
    """.stripMargin)

  trait Measure extends { this: Actor ⇒
    val NanoToSecond = 1000.0 * 1000 * 1000

    var startTime: Long = 0L
    var stopTime: Long = 0L

    var startSequenceNr = 0L;
    var stopSequenceNr = 0L;

    def startMeasure(): Unit = {
      startSequenceNr = lastSequenceNr
      startTime = System.nanoTime
    }

    def stopMeasure(): Unit = {
      stopSequenceNr = lastSequenceNr
      stopTime = System.nanoTime
      sender ! (NanoToSecond * (stopSequenceNr - startSequenceNr) / (stopTime - startTime))
    }

    def lastSequenceNr: Long
  }

  class TestPersistentActor(val persistenceId: String) extends PersistentActor with Measure {
    def receiveRecover: Receive = handle

    def receiveCommand: Receive = {
      case c @ "start" =>
        deferAsync(c) { _ => startMeasure(); sender ! "started" }
      case c @ "stop" =>
        deferAsync(c) { _ => stopMeasure() }
      case payload: String =>
        persistAsync(payload)(handle)
    }

    def handle: Receive = {
      case payload: String =>
    }
  }
}

class KafkaLoadSpec extends TestKit(ActorSystem("test", KafkaLoadSpec.config)) with ImplicitSender with WordSpecLike with Matchers with KafkaCleanup {
  import KafkaLoadSpec._

  val systemConfig = system.settings.config
  val journalConfig = new KafkaJournalConfig(systemConfig.getConfig("kafka-journal"))
  val serverConfig = new TestServerConfig(systemConfig.getConfig("test-server"))
  val server = new TestServer(serverConfig)

  override def afterAll(): Unit = {
    server.stop()
    system.shutdown()
    super.afterAll()
  }

  "A Kafka Journal" must {
    "have some reasonable throughput" in {
      val warmCycles = 100L  // set to 10000L to get reasonable results
      val loadCycles = 1000L // set to 300000L to get reasonable results

      val processor1 = system.actorOf(Props(classOf[TestPersistentActor], "test"))
      1L to warmCycles foreach { i => processor1 ! "a" }
      processor1 ! "start"
      expectMsg("started")
      1L to loadCycles foreach { i => processor1 ! "a" }
      processor1 ! "stop"
      expectMsgPF(100.seconds) { case throughput: Double ⇒ println(f"\nthroughput = $throughput%.2f persistent commands per second") }
    }
  }
}
