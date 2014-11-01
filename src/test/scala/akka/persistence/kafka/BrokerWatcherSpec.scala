package akka.persistence.kafka

import java.io.File

import akka.actor.ActorSystem
import akka.persistence.kafka.BrokerWatcher.BrokersUpdated
import akka.persistence.kafka.MetadataConsumer.Broker
import akka.persistence.kafka.server.{TestKafkaServer, TestServerConfig, TestZookeeperServer}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{ConfigValueFactory, ConfigFactory}
import kafka.utils.{VerifiableProperties, ZKConfig}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, Matchers, WordSpecLike}

object BrokerWatcherSpec {

  val dataDir = "target/test"

  val config = ConfigFactory.parseString(
   s"""
      |akka.persistence.journal.plugin = "kafka-journal"
      |akka.persistence.snapshot-store.plugin = "kafka-snapshot-store"
      |akka.test.single-expect-default = 10s
      |kafka-journal.event.producer.request.required.acks = 1
      |kafka-journal.zookeeper.connection.timeout.ms = 10000
      |kafka-journal.zookeeper.session.timeout.ms = 10000
      |test-server.zookeeper.dir = "$dataDir/zookeeper"
    """.stripMargin).withFallback(ConfigFactory.load("reference"))

  val zkClientConfig = new ZKConfig(new VerifiableProperties(configToProperties(ConfigFactory.parseString(
   s"""
      |zookeeper.connect = "localhost:${config.getInt("test-server.zookeeper.port")}"
      |zookeeper.session.timeout.ms = 6000
      |zookeeper.connection.timeout.ms = 6000
      |zookeeper.sync.time.ms = 2000
    """.stripMargin))))

  val basePort = 6667

}

abstract class BrokerWatcherSpec
  extends TestKit(ActorSystem("test", BrokerWatcherSpec.config))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  import BrokerWatcherSpec._

  FileUtils.deleteDirectory(new File(dataDir))
  val zookeeper = new TestZookeeperServer(new TestServerConfig(config.getConfig("test-server")))

  override def afterAll(): Unit = {
    zookeeper.stop()
    super.afterAll()
  }

  def spawnKafka(brokerId: Int): TestKafkaServer = {
    new TestKafkaServer(new TestServerConfig(config.getConfig("test-server")
      .withValue("kafka.broker.id", ConfigValueFactory.fromAnyRef(brokerId))
      .withValue("kafka.port", ConfigValueFactory.fromAnyRef(basePort + (brokerId - 1)))
      .withValue("kafka.log.dirs", ConfigValueFactory.fromAnyRef(dataDir + "/" + s"kafka-$brokerId"))))
  }

  def withWatcher(body: BrokerWatcher => Unit) = {
    val watcher = new BrokerWatcher(zkClientConfig, testActor)
    try {
      body(watcher)
    } finally {
      watcher.stop()
    }
  }

  def withKafka(brokerId: Int)(body: TestKafkaServer => Unit) = {
    val kafka = spawnKafka(brokerId)
    try {
      body(kafka)
    } finally {
      kafka.stop()
    }
  }

  def expectBrokers(r: Range): Unit = {
    val msg = expectMsgClass(classOf[BrokersUpdated])
    assertBrokers(msg.brokers, r)
  }

  def assertBrokers(brokers: List[Broker], r: Range): Unit = {
    assert(brokers.toSet === r.map(id => Broker("localhost", basePort + (id - 1))).toSet)
  }

}

class BrokerWatcherSpecWithKafkas extends BrokerWatcherSpec {

  val kafkas = (1 to 3).map(id =>
    spawnKafka(id)
  )

  override def afterAll(): Unit = {
    kafkas.foreach(_.stop())
    super.afterAll()
  }

  "A BrokerWatcher" must {
    "return current list of brokers on start" in {
      withWatcher { watcher =>
        val brokers = watcher.start()
        assertBrokers(brokers, 1 to 3)
      }
    }

    "notify listener when brokers are added and removed" in {
      withWatcher { watcher =>
        watcher.start()
        withKafka(4) { newBroker =>
          expectBrokers(1 to 4)
          kafkas(0).stop()
          expectBrokers(2 to 4)
        }
      }
    }
  }

}

class BrokerWatcherSpecWithoutKafkas extends BrokerWatcherSpec {

  "A BrokerWatcher" must {
    "return an empty list if no brokers are available on start" in {
      withWatcher { watcher =>
        val brokers = watcher.start()
        assert(brokers === List.empty[Broker])
      }
    }
    "notify listener when brokers eventually become available" in {
      withWatcher { watcher =>
        val brokers = watcher.start()
        assert(brokers === List.empty[Broker])
        withKafka(0) { newBroker =>
          expectBrokers(0 until 0)
          expectBrokers(0 until 1)
        }
      }
    }
  }

}
