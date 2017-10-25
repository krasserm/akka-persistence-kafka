package akka.persistence.kafka.journal

import akka.persistence.kafka._
import com.typesafe.config.Config
import kafka.utils._
import org.apache.kafka.clients.producer.ProducerConfig

class KafkaJournalConfig(config: Config) extends MetadataConsumerConfig(config) {
  val pluginDispatcher: String =
    config.getString("plugin-dispatcher")

  val eventTopicMapper: EventTopicMapper =
    CoreUtils.createObject[EventTopicMapper](config.getString("event.producer.topic.mapper.class"))

  def journalProducerConfig(): Map[String,Object] =
    configToProperties(config.getConfig("producer"),
      Map(ProducerConfig.TRANSACTIONAL_ID_CONFIG -> "akka-journal-message",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer"))

  def eventProducerConfig(): Map[String,Object] =
    configToProperties(config.getConfig("event.producer"),
      Map(ProducerConfig.TRANSACTIONAL_ID_CONFIG -> "akka-journal-event",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer"))
}
