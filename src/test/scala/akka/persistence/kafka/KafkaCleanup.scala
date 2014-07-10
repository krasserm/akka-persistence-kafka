package akka.persistence.kafka

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest._

trait KafkaCleanup extends BeforeAndAfterAll { this: Suite =>
  override protected def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File("target/journal"))
    FileUtils.deleteDirectory(new File("target/snapshots"))
  }
}
