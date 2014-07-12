package akka.persistence.kafka.server

import java.io.File
import java.util.Properties

import scala.collection.JavaConverters._

import com.typesafe.config._

import kafka.server._

import org.apache.curator.test.TestingServer

object TestServerConfig extends TestServerConfig(ConfigFactory.load().getConfig("test-server"))

class TestServerConfig(config: Config) {
  object zookeeper {
    val port = config.getInt("zookeeper.port")
    val dir = config.getString("zookeeper.dir")
  }

  def kafkaConfig: KafkaConfig =
    new KafkaConfig(toProperties(config.getConfig("kafka"),
      Map("zookeeper.connect" -> s"localhost:${zookeeper.port}", "hostname" -> "localhost")))

  private def toProperties(config: Config, extra: Map[String, String] = Map.empty): Properties = {
    val properties = new Properties()

    config.entrySet.asScala.foreach { entry =>
      properties.put(entry.getKey, entry.getValue.unwrapped.toString)
    }

    extra.foreach {
      case (k, v) => properties.put(k, v)
    }

    properties
  }
}

class TestServer {
  val zookeeper = new TestZookeeperServer()
  val kafka = new TestKafkaServer()

  def stop(): Unit = {
    kafka.stop()
    zookeeper.stop()
  }
}

class TestZookeeperServer {
  import TestServerConfig._

  private val server: TestingServer =
    new TestingServer(zookeeper.port, new File(zookeeper.dir))

  def stop(): Unit = server.stop()
}

class TestKafkaServer {
  private val server: KafkaServer =
    new KafkaServer(TestServerConfig.kafkaConfig)

  server.startup()

  def stop(): Unit = {
    server.shutdown()
    server.awaitShutdown()
  }
}
