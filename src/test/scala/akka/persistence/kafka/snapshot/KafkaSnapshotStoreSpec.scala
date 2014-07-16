package akka.persistence.kafka.snapshot

import com.typesafe.config.ConfigFactory

import akka.persistence.snapshot.SnapshotStoreSpec
import akka.persistence.kafka.KafkaCleanup
import akka.persistence.kafka.server._

class KafkaSnapshotStoreSpec extends SnapshotStoreSpec with KafkaCleanup {
  lazy val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "kafka-journal"
      |akka.persistence.snapshot-store.plugin = "kafka-snapshot-store"
      |akka.test.single-expect-default = 10s
      |kafka-journal.event.producer.request.required.acks = 1
      |test-server.zookeeper.dir = target/test/zookeeper
      |test-server.kafka.log.dirs = target/test/kafka
    """.stripMargin)

  val systemConfig = system.settings.config
  val serverConfig = new TestServerConfig(systemConfig.getConfig("test-server"))
  val server = new TestServer(serverConfig)

  override protected def afterAll(): Unit = {
    server.stop()
    super.afterAll()
  }
}
