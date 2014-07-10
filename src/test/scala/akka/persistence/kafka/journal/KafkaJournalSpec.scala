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
    """.stripMargin)

  val server = new TestServer()

  override protected def afterAll(): Unit = {
    server.stop()
    super.afterAll()
  }
}
