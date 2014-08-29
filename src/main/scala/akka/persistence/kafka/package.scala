package akka.persistence

import scala.collection.JavaConverters._

import java.util.Properties

import com.typesafe.config.Config

package object kafka {
  def journalTopic(persistenceId: String): String =
    persistenceId.replaceAll("[^\\w\\._-]", "_")

  def configToProperties(config: Config, extra: Map[String, String] = Map.empty): Properties = {
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
