package akka.persistence.kafka.snapshot

import akka.persistence.kafka._
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.ProducerConfig

class KafkaSnapshotStoreConfig(config: Config) extends MetadataConsumerConfig(config) {
  val prefix: String =
    config.getString("prefix")

  val ignoreOrphan: Boolean =
    config.getBoolean("ignore-orphan")

  def producerConfig(): Map[String,Object] =
    configToProperties(config.getConfig("producer"),
      Map(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer"))
}
