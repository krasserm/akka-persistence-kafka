package akka.persistence.kafka.journal

import scala.util._

import akka.actor.Actor

import kafka.api.TopicMetadataRequest
import kafka.consumer._
import kafka.utils._

import org.I0Itec.zkclient.ZkClient

object KafkaMetadata {
  object Broker {
    def toString(brokers: List[Broker]) = brokers.mkString(",")
  }

  case class Broker(host: String, port: Int) {
    override def toString = s"${host}:${port}"
  }
}

trait KafkaMetadata { this: Actor =>
  import KafkaMetadata._

  val configRaw = context.system.settings.config.getConfig("kafka-journal")
  val config = new KafkaJournalConfig(configRaw)
  val brokers = readBrokers(config.zookeeperConfig.zkConnect)

  def leaderFor(topic: String, brokers: List[Broker]): Option[Broker] = {
    brokers match {
      case Nil => None
      case Broker(host, port) :: brokers =>
        Try(leaderFor(topic, host, port)) match {
          case Success(l) => l
          case Failure(e) => leaderFor(topic, brokers)
        }
    }
  }

  def leaderFor(topic: String, host: String, port: Int): Option[Broker] = {
    import ConsumerConfig.DefaultClientId
    import config.journalConsumerConfig._

    val consumer = new SimpleConsumer(host, port, socketTimeoutMs, socketReceiveBufferBytes, DefaultClientId)
    val request = new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, 0, DefaultClientId, List(topic))
    val response = try {
      consumer.send(request)
    } finally {
      consumer.close()
    }

    val leaders = for {
      tmd <- response.topicsMetadata
      pmd <- tmd.partitionsMetadata if pmd.partitionId == config.partition
      ldr <- pmd.leader
    } yield Broker(ldr.host, ldr.port)

    leaders.headOption
  }

  def readBrokers(zkConnect: String): List[Broker] = {
    val zkConfig = config.zookeeperConfig
    val client = new ZkClient(
      zkConfig.zkConnect,
      zkConfig.zkSessionTimeoutMs,
      zkConfig.zkConnectionTimeoutMs,
      ZKStringSerializer)

    try {
      ZkUtils.getAllBrokersInCluster(client).map(b => Broker(b.host, b.port)).toList
    } finally {
      client.close()
    }
  }
}
