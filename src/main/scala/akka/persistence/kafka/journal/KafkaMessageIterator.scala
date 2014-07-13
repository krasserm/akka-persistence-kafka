package akka.persistence.kafka.journal

import kafka.api.FetchRequestBuilder
import kafka.consumer._
import kafka.message._

object KafkaMessageIterator {
  def payloadBytes(m: Message): Array[Byte] = {
    val payload = m.payload
    val payloadBytes = Array.ofDim[Byte](payload.limit())

    payload.get(payloadBytes)
    payloadBytes
  }
}

class KafkaMessageIterator(host: String, port: Int, topic: String, partition: Int, offset: Long, consumerConfig: ConsumerConfig) extends Iterator[Message] {
  import consumerConfig._

  val consumer = new SimpleConsumer(host, port, socketTimeoutMs, socketReceiveBufferBytes, clientId)
  var iter = iterator(offset)
  var read = 0
  var nxto = offset

  def iterator(offset: Long): Iterator[MessageAndOffset] = {
    val request = new FetchRequestBuilder().addFetch(topic, partition, offset, fetchMessageMaxBytes).build()
    val response = consumer.fetch(request)
    response.messageSet(topic, partition).iterator
  }

  def next(): Message = {
    val mo = iter.next()
    read += 1
    nxto = mo.nextOffset
    mo.message
  }

  @annotation.tailrec
  final def hasNext: Boolean =
    if (iter.hasNext) {
      true
    } else if (read == 0) {
      close()
      false
    } else {
      iter = iterator(nxto)
      read = 0
      hasNext
    }

  def close(): Unit = {
    consumer.close()
  }
}
