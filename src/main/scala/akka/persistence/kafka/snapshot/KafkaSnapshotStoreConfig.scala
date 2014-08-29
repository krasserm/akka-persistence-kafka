package akka.persistence.kafka.snapshot

import akka.persistence.kafka._
import akka.persistence.kafka.MetadataConsumer.Broker

import com.typesafe.config.Config

import kafka.producer.ProducerConfig

class KafkaSnapshotStoreConfig(config: Config) extends MetadataConsumerConfig(config) {
  val prefix: String =
    config.getString("prefix")

  val ignoreOrphan: Boolean =
    config.getBoolean("ignore-orphan")

  def producerConfig(brokers: List[Broker]): ProducerConfig =
    new ProducerConfig(configToProperties(config.getConfig("producer"),
      Map("metadata.broker.list" -> Broker.toString(brokers), "partition" -> config.getString("partition"))))
}
