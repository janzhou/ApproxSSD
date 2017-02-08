package org.janzhou.nvmr.storage

import org.janzhou.native._
import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

class Driver(val _file: String) extends AutoCloseable {
  var file = new RandomAccessFile(_file, "rw")
  var channel = file.getChannel()

  def flush = {
    file.getFD().sync()
  }

  def write(offset: Long, buf: ByteBuffer): Long = {
     try{
      buf.rewind()
      channel.position(offset)
      while (buf.hasRemaining()) {
        channel.write(buf)
      }
      return buf.limit()
    } catch {
      case e: IOException => e.printStackTrace()
    }

    0
  }

  def writeObject(offset: Long, obj: Any): Long = {
    try{
      val buf = Serializer.serialize(obj)
      buf.rewind()
      channel.position(offset)
      while (buf.hasRemaining()) {
        channel.write(buf)
      }
      return buf.limit()
    } catch {
      case e: IOException => e.printStackTrace()
    }

    0
  }

  def read(offset: Long, length: Long): ByteBuffer = {
    val buf = ByteBuffer.allocate(length.toInt)
    try {
      var nread = 0
      channel.position(offset)
      do {
        nread = channel.read(buf)
      } while (nread != -1 && buf.hasRemaining())
    } catch {
      case e: IOException => e.printStackTrace()
    }
    buf.rewind()
    buf
  }

  def readObject[T: ClassTag](offset: Long, length: Long): T = {
    val buf = read(offset, length)
    Serializer.deserialize[T](buf)
  }

  val fd = libc.run().open(_file, 1)
  val size = libc.run().lseek(fd, 0L, libc.SEEK_END)

  def trim(offset:Long, length:Long):Int = { // length = N * page size
    val range = Array(offset, length)
    if ( fd != 0 ) {
      libc.run().ioctl(fd, libc.IOCTL_TRIM, range)
    } else -1
  }

  @Override
  def close() = {
    println("autoclose")
    if ( fd != 0 ) {
      libc.run().close(fd)
    }
    channel.close()
  }
}
