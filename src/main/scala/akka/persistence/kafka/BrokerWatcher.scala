
package akka.persistence.kafka

import akka.actor.ActorRef
import akka.persistence.kafka.BrokerWatcher.BrokersUpdated
import akka.persistence.kafka.MetadataConsumer.Broker
import kafka.utils.{ZkUtils, ZKStringSerializer, ZKConfig}
import org.I0Itec.zkclient.ZkClient

object BrokerWatcher {

  case class BrokersUpdated(brokers: List[Broker])

}

class BrokerWatcher(zkConfig: ZKConfig, listener: ActorRef) {

  lazy val zkClient = new ZkClient(
    zkConfig.zkConnect,
    zkConfig.zkSessionTimeoutMs,
    zkConfig.zkConnectionTimeoutMs,
    ZKStringSerializer)

  lazy val childWatcher = new ChildDataWatcher[String](zkClient, ZkUtils.BrokerIdsPath, { d =>
    listener ! BrokersUpdated(buildBrokers(d))
  })

  def start(): List[Broker] = {
    buildBrokers(childWatcher.start())
  }

  def stop(): Unit = {
    childWatcher.stop()
    zkClient.close()
  }

  private def buildBrokers(d: Map[String, String]): List[Broker] = {
    d.values.map(Broker.fromString).flatMap(x => x).toList
  }
}