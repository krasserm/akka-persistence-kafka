package akka.persistence.kafka.journal

import com.typesafe.config.ConfigFactory

import akka.persistence.journal.JournalSpec
import akka.persistence.kafka.KafkaCleanup
import akka.persistence.kafka.server._

class KafkaJournalSpec extends JournalSpec (
  config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "kafka-journal"
      |akka.persistence.snapshot-store.plugin = "kafka-snapshot-store"
      |akka.test.single-expect-default = 10s
      |kafka-journal.event.producer.request.required.acks = 1
      |test-server.zookeeper.dir = target/test/zookeeper
      |test-server.kafka.log.dirs = target/test/kafka
    """.stripMargin)) with KafkaCleanup {

  val systemConfig = system.settings.config
  val serverConfig = new TestServerConfig(systemConfig.getConfig("test-server"))
  val server = new TestServer(serverConfig)

  override protected def afterAll(): Unit = {
    server.stop()
    super.afterAll()
  }
}
