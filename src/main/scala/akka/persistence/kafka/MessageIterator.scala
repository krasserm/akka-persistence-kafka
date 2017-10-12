package akka.persistence.kafka

import kafka.api.FetchRequestBuilder
import kafka.common.ErrorMapping
import kafka.consumer._
import kafka.message._
import org.apache.kafka.common.protocol.Errors

object MessageUtil {
  def payloadBytes(m: Message): Array[Byte] = {
    val payload = m.payload
    val payloadBytes = Array.ofDim[Byte](payload.limit())

    payload.get(payloadBytes)
    payloadBytes
  }
}

class MessageIterator(host: String, port: Int, topic: String, partition: Int, offset: Long, consumerConfig: ConsumerConfig) extends Iterator[Message] {
  import consumerConfig._
  import ErrorMapping._

  val consumer = new SimpleConsumer(host, port, socketTimeoutMs, socketReceiveBufferBytes, clientId)
  var iter = iterator(offset)
  var readMessages = 0
  var nextOffset = offset

  def iterator(offset: Long): Iterator[MessageAndOffset] = {
    val request = new FetchRequestBuilder().addFetch(topic, partition, offset, fetchMessageMaxBytes).build()
    val response = consumer.fetch(request)

    response.error(topic, partition) match {
      case Errors.NONE => response.messageSet(topic, partition).iterator
      case anError => throw exceptionFor(anError.code())
    }
  }

  def next(): Message = {
    val mo = iter.next()
    readMessages += 1
    nextOffset = mo.nextOffset
    mo.message
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
