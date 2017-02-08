package org.janzhou.nvmr.storage

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

object Serializer {
  def serialize(obj:Any): ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    out.writeObject(obj)
    val byteArray = bos.toByteArray()
    ByteBuffer.wrap(byteArray)
  }

  def deserialize[T: ClassTag](buf:ByteBuffer): T = {
    buf.rewind()
    val array = buf.array()
    val inputStream = new ByteArrayInputStream(array)
    val objectInputStream = new ObjectInputStream(inputStream)
    val obj = objectInputStream.readObject()
    objectInputStream.close()
    obj.asInstanceOf[T]
  }
}
