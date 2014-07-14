package akka.persistence.kafka.server

import java.io.File
import java.util.Properties

import scala.collection.JavaConverters._

import com.typesafe.config._

import kafka.server._

import org.apache.curator.test.TestingServer

object TestServerConfig {
  def load(): TestServerConfig =
    load("application")

  def load(resource: String): TestServerConfig =
    new TestServerConfig(ConfigFactory.load(resource).getConfig("test-server"))
}

class TestServerConfig(config: Config) {
  object zookeeper {
    val port: Int =
      config.getInt("zookeeper.port")

    val dir: String =
      config.getString("zookeeper.dir")
  }

  val kafka: KafkaConfig =
    new KafkaConfig(toProperties(config.getConfig("kafka"),
      Map("zookeeper.connect" -> s"localhost:${zookeeper.port}", "host.name" -> "localhost")))

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

class TestServer(config: TestServerConfig = TestServerConfig.load()) {
  val zookeeper = new TestZookeeperServer(config)
  val kafka = new TestKafkaServer(config)

  def stop(): Unit = {
    kafka.stop()
    zookeeper.stop()
  }
}

class TestZookeeperServer(config: TestServerConfig) {
  import config._

  private val server: TestingServer =
    new TestingServer(zookeeper.port, new File(zookeeper.dir))

  def stop(): Unit = server.stop()
}

class TestKafkaServer(config: TestServerConfig) {
  private val server: KafkaServer =
    new KafkaServer(config.kafka)

  server.startup()

  def stop(): Unit = {
    server.shutdown()
    server.awaitShutdown()
  }
}

