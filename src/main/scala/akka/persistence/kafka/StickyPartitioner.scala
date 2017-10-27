package akka.persistence.kafka

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

class StickyPartitioner() extends Partitioner {
  var partition: Option[Int] = None

  override def partition(topic: String, key: scala.Any, keyBytes: Array[Byte], value: scala.Any, valueBytes: Array[Byte], cluster: Cluster) = partition.get

  override def configure(configs: util.Map[String, _]) = {
    partition = Some(configs.get("partition").asInstanceOf[String].toInt)
  }

  override def close() = {}
}
