package akka.persistence.kafka

import scala.util._
import kafka.api._
import kafka.common._
import kafka.consumer._
import kafka.utils._
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.protocol.Errors

object MetadataConsumer {
  object Broker {
    def toString(brokers: List[Broker]) = brokers.mkString(",")

    def fromString(asString: String): Option[Broker] = {
      Json.parseFull(asString) match {
        case Some(m) =>
          val brokerInfo = m.asInstanceOf[Map[String, Any]]
          val host = brokerInfo.get("host").get.asInstanceOf[String]
          val port = brokerInfo.get("port").get.asInstanceOf[Int]
          Some(Broker.apply(host, port))
        case None => None
      }
    }
  }

  case class Broker(host: String, port: Int) {
    override def toString = s"${host}:${port}"
  }
}

trait MetadataConsumer {
  import MetadataConsumer._

  val config: MetadataConsumerConfig

  def leaderFor(topic: String, brokers: List[Broker]): Option[Broker] = brokers match {
    case Nil =>
      throw new IllegalArgumentException("empty broker list")
    case Broker(host, port) :: Nil =>
      leaderFor(host, port, topic)
    case Broker(host, port) :: brokers =>
      Try(leaderFor(host, port, topic)) match {
        case Failure(e) => leaderFor(topic, brokers) // failover
        case Success(l) => l
      }
  }

  def leaderFor(host: String, port: Int, topic: String): Option[Broker] = {
    import config.consumerConfig._
    import ErrorMapping._

    val consumer = new SimpleConsumer(host, port, socketTimeoutMs, socketReceiveBufferBytes, clientId)
    val request = new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, 0, clientId, List(topic))
    val response = try { consumer.send(request) } finally { consumer.close() }
    val topicMetadata = response.topicsMetadata(0)

    try {
      topicMetadata.error match {
        case Errors.LEADER_NOT_AVAILABLE => None
        case Errors.NONE => topicMetadata.partitionsMetadata.filter(_.partitionId == config.partition)(0).leader.map(leader => Broker(leader.host, leader.port))
        case anError => throw anError.exception()
      }
    } finally {
      consumer.close()
    }
  }

  def offsetFor(host: String, port: Int, topic: String, partition: Int): Long = {
    import config.consumerConfig._
    import ErrorMapping._

    val consumer = new SimpleConsumer(host, port, socketTimeoutMs, socketReceiveBufferBytes, clientId)
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
  }
}