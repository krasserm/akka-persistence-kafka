package akka.persistence.kafka

import kafka.message._
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

class MessageIterator(consumerConfig:Map[String,Object], topic: String, partition: Int, offset: Long) extends Iterator[ConsumerRecord[String, Array[Byte]]] {

  val consumer = new KafkaConsumer[String, Array[Byte]](consumerConfig.asJava)
  var iter = iterator(offset)
  var readMessages = 0
  var nextOffset = offset

  def iterator(offset: Long): Iterator[ConsumerRecord[String, Array[Byte]]] = {
    val tp = new TopicPartition(topic,partition)
    consumer.assign(List(tp).asJava)
    consumer.seek(tp,offset)
    val it = consumer.poll(100).iterator().asScala
    it
  }

  def next(): ConsumerRecord[String, Array[Byte]] = {
    val mo = iter.next()
    readMessages += 1
    nextOffset = mo.offset() + 1
    mo
  }

  @annotation.tailrec
  final def hasNext: Boolean =
    if (iter.hasNext) {
      true
    } else if (readMessages == 0) {
      close()
      false
    } else {
      iter = iterator(nextOffset)
      readMessages = 0
      hasNext
    }

  def close(): Unit = {
    consumer.close()
  }
}
