package akka.persistence.kafka

import com.typesafe.config.Config

import kafka.consumer.ConsumerConfig
import kafka.utils._

class MetadataConsumerConfig(config: Config) {
  val partition: Int =
    config.getInt("partition")

  val zookeeperConfig: ZKConfig =
    new ZKConfig(new VerifiableProperties(configToProperties(config)))

  val consumerConfig: ConsumerConfig =
    new ConsumerConfig(configToProperties(config.getConfig("consumer"),
      Map("zookeeper.connect" -> zookeeperConfig.zkConnect, "group.id" -> "snapshot")))
}
