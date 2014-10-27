package akka.persistence.kafka

import java.util

import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.I0Itec.zkclient.{IZkDataListener, ZkClient, IZkChildListener}

private [kafka] class ChildDataWatcher[T](
  zkClient: ZkClient,
  path: String,
  cb: (Map[String, T]) => Unit)
  extends IZkChildListener
  with IZkDataListener {

  import scala.collection.JavaConverters._

  private var childDataMap = Map[String, T]()

  def start(): Map[String, T] = {
    val eventLock = zkClient.getEventLock
    try {
      eventLock.lock()
      val children = zkClient.subscribeChildChanges(path, this)
      if (children != null) {
        handleChildChange(path, children, invokeCallback = false)
      }
      childDataMap
    } finally {
      eventLock.unlock()
    }
  }

  def stop(): Unit = {
    val eventLock = zkClient.getEventLock
    try {
      eventLock.lock()
      zkClient.unsubscribeChildChanges(path, this)
      childDataMap.foreach {
        case (c, _) => zkClient.unsubscribeDataChanges(childPath(c), this)
      }
    } finally {
      eventLock.unlock()
    }
  }

  def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    handleChildChange(parentPath, currentChilds, invokeCallback = true)
  }

  def handleChildChange(parentPath: String, currentChilds: util.List[String], invokeCallback: Boolean): Unit = {
    val oldChildren = childDataMap.keySet
    // WARNING: if the currentChilds is null we can treat it as an empty set, however, we are never going to be notified
    // if the parentPath is re-created.
    val newChildren = if (currentChilds != null) currentChilds.asScala.toSet else Set.empty[String]
    val childrenAdded = newChildren.diff(oldChildren)
    val childrenRemoved = oldChildren.diff(newChildren)

    childrenRemoved.foreach { c =>
      zkClient.unsubscribeDataChanges(childPath(c), this)
      childDataMap = childDataMap - c
    }

    childrenAdded.foreach { c =>
      zkClient.subscribeDataChanges(childPath(c), this)
      try {
        val data = zkClient.readData[T](childPath(c))
        childDataMap = childDataMap + (c -> data)
      } catch {
        case e: ZkNoNodeException =>
          zkClient.unsubscribeDataChanges(childPath(c), this)
      }
    }

    if (invokeCallback)
      cb(childDataMap)
  }

  def handleDataChange(path: String, data: Object): Unit = {
    childDataMap = childDataMap + (childId(path) -> data.asInstanceOf[T])
    cb(childDataMap)
  }

  def handleDataDeleted(path: String): Unit = {
    // znode deletion is handled by handleChildChange
  }

  private def childId(path: String): String = path.substring(this.path.size + 1)

  private def childPath(child: String): String = path + "/" + child
}
