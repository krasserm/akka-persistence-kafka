package akka.persistence.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

trait MetadataConsumer {

  def nextOffsetFor(config:Map[String,Object], topic: String, partition: Int): Long = {
    val tp = new TopicPartition(topic,partition)
    val consumer = new KafkaConsumer[String, Array[Byte]](config.asJava)
    try {
      consumer.assign(List(tp).asJava)
      consumer.endOffsets(List(tp).asJava).get(tp)
    } finally {
      consumer.close()
    }
  }

}