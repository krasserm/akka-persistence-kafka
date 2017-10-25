package akka.persistence.kafka.server

import java.util.Properties

import com.typesafe.config._
import kafka.integration.KafkaServerTestHarness
import kafka.server._
import kafka.utils.{TestUtils, ZkUtils}

import scala.collection.JavaConverters._

object ConfigurationOverride {

  // Give a chance (mainly for tests) to override classpath application
  // configuration
  protected[kafka] var configApp: Config = _

}

object Configuration {

  def init(): Unit = {
    // We just want to initialize the configuration, which is now done
  }

  val configApp = Option(ConfigurationOverride.configApp).getOrElse(ConfigFactory.load())

}

class TestServer(config: Config) extends KafkaServerTestHarness {
  val kafkaConfig = config.getConfig("kafka")

  private def serverProps() = {
    val serverProps = new Properties()
    kafkaConfig.entrySet.asScala.foreach { entry ⇒
      serverProps.put(entry.getKey, entry.getValue.unwrapped.toString)
    }
    serverProps
  }

  override def generateConfigs: Seq[KafkaConfig] = {
    Seq(TestUtils.createBrokerConfig(nodeId = 1, zkConnect = zkConnect, port = kafkaConfig.getInt("port")))
      .map(KafkaConfig.fromProps(_, serverProps()))
  }
}

import org.scalatest._
trait KafkaTest extends BeforeAndAfterAll { this: Suite ⇒

  var server:TestServer = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val serverConfig = Configuration.configApp.getConfig("test-server")
    server = new TestServer(serverConfig)
    server.setUp()
  }

  override def afterAll(): Unit = {
    server.tearDown()
    super.afterAll()
  }
}
