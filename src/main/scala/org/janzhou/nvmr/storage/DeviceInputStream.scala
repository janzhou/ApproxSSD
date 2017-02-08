package org.janzhou.nvmr.storage

import java.io._

class DeviceInputStream(dev:RandomAccessFile) extends InputStream {
  override def read():Int = {
    dev.read()
  }

  override def read(b:Array[Byte]):Int = {
    dev.read(b)
  }

  override def read(b:Array[Byte], off:Int, len:Int):Int = {
    dev.read(b, off, len)
  }

  def seek(pos:Long) = {
    dev.seek(pos)
  }

  def offset:Long = {
    dev.getFilePointer()
  }
}
