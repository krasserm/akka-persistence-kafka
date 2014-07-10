package akka.persistence.kafka.server

import java.io.File
import java.util.Properties

import kafka.server._

import org.apache.curator.test.TestingServer

class TestServer {
  val zookeeper = new TestZookeeperServer()
  val kafka = new TestKafkaServer()

  def stop(): Unit = {
    kafka.stop()
    zookeeper.stop()
  }
}

object TestZookeeperServer {
  val port: Int = 2181
}

class TestZookeeperServer {
  import TestZookeeperServer._

  private val server: TestingServer = new TestingServer(port, new File("target/journal/zookeeper"))

  def stop(): Unit = server.stop()
}

object TestKafkaServer {
  val port = 6667
}

class TestKafkaServer {
  val props = new Properties()

  props.put("broker.id", "1")
  props.put("num.partitions", "3")
  props.put("hostname", "localhost")
  props.put("port", s"${TestKafkaServer.port}")
  props.put("log.dirs", "target/journal/kafka")
  props.put("log.index.size.max.bytes", "1024")
  props.put("zookeeper.connect", s"localhost:${TestZookeeperServer.port}")

  val server = new KafkaServer(new KafkaConfig(props))

  server.startup()

  def stop(): Unit = {
    server.shutdown()
    server.awaitShutdown()
  }
}
