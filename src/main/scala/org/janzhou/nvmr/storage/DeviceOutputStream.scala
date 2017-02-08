package org.janzhou.nvmr.storage

import java.io._

class DeviceOutputStream(dev:RandomAccessFile, _offset:Long=0L) extends OutputStream {
  private var head = _offset

  def offset:Long = head

  override def flush = {
    dev.getFD().sync()
  }

  override def write(b:Int) = {
    dev.seek(head)
    val ret = dev.writeInt(b)
    head = dev.getFilePointer()
    ret
  }

  override def write(b:Array[Byte]) = {
    dev.seek(head)
    val ret = dev.write(b)
    head = dev.getFilePointer()
    ret
  }

  override def write(b:Array[Byte], off:Int, len:Int) = {
    dev.seek(head)
    val ret = dev.write(b, off, len)
    head = dev.getFilePointer()
    ret
  }

  def seek(pos:Long) = {
    head = pos
  }

  def size:Long = {
    dev.length
  }
}
