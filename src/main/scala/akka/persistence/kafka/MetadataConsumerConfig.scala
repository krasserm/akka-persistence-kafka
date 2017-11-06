package akka.persistence.kafka

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig


class MetadataConsumerConfig(config: Config) {
  val partition: Int =
    config.getInt("partition")

  val snapshotConsumerConfig: Map[String,Object] =
    configToProperties(config.getConfig("consumer"),
      Map(ConsumerConfig.GROUP_ID_CONFIG -> "journal-snapshot-reader",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer"))

  lazy val offsetConsumerConfig: Map[String,Object] =
    snapshotConsumerConfig ++ Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_uncommitted")

  lazy val txnAwareConsumerConfig: Map[String,Object] =
    snapshotConsumerConfig ++ Map(ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
}
