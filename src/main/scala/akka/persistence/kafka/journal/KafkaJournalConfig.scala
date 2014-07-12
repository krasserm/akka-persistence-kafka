package akka.persistence.kafka.journal

import java.util.Properties

import scala.collection.JavaConverters._

import akka.persistence.kafka.EventTopicMapper

import com.typesafe.config.Config

import kafka.consumer.ConsumerConfig
import kafka.producer.ProducerConfig
import kafka.utils._

class KafkaJournalConfig(config: Config) {
  val replayDispatcher: String =
    config.getString("replay-dispatcher")

  val partition: Int =
    config.getInt("partition")

  val eventTopicMapper: EventTopicMapper =
    Utils.createObject[EventTopicMapper](config.getString("event.producer.topic.mapper.class"))

  val zookeeperConfig: ZKConfig =
    new ZKConfig(new VerifiableProperties(toProperties(config)))

  val journalConsumerConfig: ConsumerConfig =
    new ConsumerConfig(toProperties(config.getConfig("consumer"),
      Map("zookeeper.connect" -> zookeeperConfig.zkConnect, "group.id" -> "journal")))

  def journalProducerConfig(brokers: List[String]): ProducerConfig =
    new ProducerConfig(toProperties(config.getConfig("producer"),
      Map("metadata.broker.list" -> brokers.mkString(","), "partition" -> config.getString("partition"))))

  def eventProducerConfig(brokers: List[String]): ProducerConfig =
    new ProducerConfig(toProperties(config.getConfig("event.producer"),
      Map("metadata.broker.list" -> brokers.mkString(","))))

  private def toProperties(config: Config, extra: Map[String, String] = Map.empty): Properties = {
    val properties = new Properties()

    config.entrySet.asScala.foreach { entry =>
      properties.put(entry.getKey, entry.getValue.unwrapped.toString)
    }

    extra.foreach {
      case (k, v) => properties.put(k, v)
    }

    properties
  }
}
