package akka.persistence.kafka.journal

import com.typesafe.config.ConfigFactory

import akka.persistence.journal.JournalSpec
import akka.persistence.kafka.KafkaCleanup
import akka.persistence.kafka.server._

class KafkaJournalSpec extends JournalSpec with KafkaCleanup {
  lazy val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "kafka-journal"
      |akka.persistence.snapshot-store.local.dir = "target/snapshots"
      |akka.test.single-expect-default = 10s
      |kafka-journal.event.producer.request.required.acks = 1
      |test-server.zookeeper.dir = target/journal/zookeeper
      |test-server.kafka.log.dirs = target/journal/kafka
    """.stripMargin)

  val systemConfig = system.settings.config
  val serverConfig = new TestServerConfig(systemConfig.getConfig("test-server"))
  val server = new TestServer(serverConfig)

  override protected def afterAll(): Unit = {
    server.stop()
    super.afterAll()
  }
}
