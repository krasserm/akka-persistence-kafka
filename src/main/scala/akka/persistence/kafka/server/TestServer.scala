package akka.persistence.kafka.server

import java.io.File

import akka.persistence.kafka._

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
    new KafkaConfig(configToProperties(config.getConfig("kafka"),
      Map("zookeeper.connect" -> s"localhost:${zookeeper.port}", "host.name" -> "localhost")))
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

