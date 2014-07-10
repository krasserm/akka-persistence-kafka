package akka.persistence.kafka.journal

import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties

class StickyPartitioner(props: VerifiableProperties) extends Partitioner {
  def partition(key: Any, numPartitions: Int): Int = 0
}
