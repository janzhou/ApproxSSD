package org.janzhou.nvmr.storage

import java.io._
import scala.reflect.ClassTag
import org.janzhou.native._
import org.janzhou.nvmr._

class Device(_dev:String) extends Storage {
  val dev = new RandomAccessFile(_dev, "rw")
  val in = new DeviceInputStream(dev)

  private var loadSize = 0L
  private var storeSize = 0L

  def getLoadSize = loadSize
  def getStoreSize = storeSize

  def load[T: ClassTag](loc:(Long, Long)): T = {
    val obj = this.synchronized {
      in.seek(loc._1)
      assert(in.offset == loc._1)
      loadSize += loc._2
      val obj = new ObjectInputStream(in).readObject()
      assert(in.offset == loc._1 + loc._2)
      obj
    }
    obj.asInstanceOf[T]
  }

  val out = new DeviceOutputStream(dev, load[Long]((0, 82L)))

  def flush = {
    out.flush
  }

  def store(obj:Any):(Long, Long) = {
    val offset = out.offset
    val length = this.synchronized {
      val outObj = new ObjectOutputStream(out)
      outObj.writeObject(obj)
      outObj.flush
      val length = out.offset - offset
      storeSize += length
      length
    }
    (offset, length)
  }

  def trim(offset:Long, length:Long):Int = {
    val range = Array(offset, length)

    val fd = libc.run().open(_dev, 1)

    val ret = if ( fd != 0 ) {
      libc.run().ioctl(fd, libc.IOCTL_TRIM, range)
    } else -1

    libc.run().close(fd)
    ret
  }

  def trim(loc:(Long, Long)):Int = trim(loc._1, loc._2)

  def size:Long = {
    dev.length
  }

  override def close = {
    val offset = out.offset
    out.seek(0L)
    val loc = store(offset)
    console.debug("device " + _dev + " finalize " + loc + " " + offset)
    out.flush
  }

  console.debug("Device: " + _dev + " Offset: " + out.offset)
}
