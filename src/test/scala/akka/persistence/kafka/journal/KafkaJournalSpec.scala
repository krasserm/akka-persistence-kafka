package akka.persistence.kafka.journal

import com.typesafe.config.ConfigFactory

import akka.persistence.journal.JournalSpec
import akka.persistence.kafka.server._
import akka.persistence.CapabilityFlag

class KafkaJournalSpec extends JournalSpec (
  config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "kafka-journal"
      |akka.persistence.snapshot-store.plugin = "kafka-snapshot-store"
      |akka.test.single-expect-default = 10s
      |kafka-journal.event.producer.request.required.acks = 1
    """.stripMargin)) with KafkaTest {

  val systemConfig = system.settings.config
  ConfigurationOverride.configApp = config.withFallback(systemConfig)

  override def supportsAtomicPersistAllOfSeveralEvents: Boolean = false
  
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.off()

}
