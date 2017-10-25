package akka.persistence.kafka

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors

import scala.collection.JavaConverters._

trait MetadataConsumer {

  def offsetFor(config:Map[String,Object], topic: String, partition: Int): Long = {

    val tp = new TopicPartition(topic,partition)
    val consumer = new KafkaConsumer[String, Array[Byte]](config.asJava)
    try {
      consumer.assign(List(tp).asJava)
      consumer.endOffsets(List(tp).asJava).get(tp)
    } finally {
      consumer.close()
    }
  }

  /*def oldOffsetFor(config:Map[String,Object], topic: String, partition: Int): Long = {

    val consumer = new SimpleConsumer("localhost", 6667, 10000, 65536, "old-offset")
    val offsetRequest = OffsetRequest(Map(TopicAndPartition(topic, partition) -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
    val offsetResponse = try { consumer.getOffsetsBefore(offsetRequest) } finally { consumer.close() }
    val offsetPartitionResponse = offsetResponse.partitionErrorAndOffsets(TopicAndPartition(topic, partition))

    try {
      offsetPartitionResponse.error match {
        case Errors.NONE => offsetPartitionResponse.offsets.head
        case anError => throw anError.exception()
      }
    } finally {
      consumer.close()
    }
  }*/
}