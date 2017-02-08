package org.janzhou.nvmr.storage

import java.io.File
import scala.reflect.ClassTag

import org.janzhou.nvmr.config

class DriverStorage(_device: String) extends Storage {
  val driver = new Driver(_device)

  var head:Long = driver.readObject[Long](0L, 82L)

  def flush = driver.flush

  def store(obj:Any):(Long, Long) = {
    val buf = Serializer.serialize(obj)
    val offset:Long = head
    val length:Long = buf.limit()
    head += length
    assert(head <= driver.size)
    driver.write(offset, buf)
    (offset, length)
  }

  def load[T: ClassTag](loc:(Long, Long)): T = driver.readObject[T](loc._1, loc._2)
  def trim(loc:(Long, Long)) = driver.trim(loc._1, loc._2)

  override def close = {
    driver.writeObject(0L, head)
  }
}

abstract class Storage {
  def load[T: ClassTag](loc:(Long, Long)): T
  def store(obj:Any):(Long, Long)
  def trim(loc:(Long, Long)):Int
  def close
}

object Storage {
  val logs:List[Storage] = config.getStringList("storage.devices").map(dev => {
    config.getString("storage.engine") match {
      case "kryo" => new KryoDevice(dev)
      case "device" => new Device(dev)
      case "driver" => new DriverStorage(dev)
    }
  })
}
