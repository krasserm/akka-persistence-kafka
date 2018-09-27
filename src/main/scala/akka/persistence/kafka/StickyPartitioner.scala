package akka.persistence.kafka

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster


class StickyPartitioner() extends Partitioner {
  var partition:Int = 0

  override def partition(topic: String, key: scala.Any, keyBytes: Array[Byte], value: scala.Any, valueBytes: Array[Byte], cluster: Cluster): Int = partition

  override def configure(configs: util.Map[String, _]): Unit = {
    partition = configs.get("partition").toString.toInt
  }

  override def close(): Unit = {}
}
