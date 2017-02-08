package org.janzhou.nvmr.pmemory

import scala.reflect.ClassTag

class Plog( _file:String, _init:Boolean = false ) {
  private val pmemory = new Pmemory(_file)
  private var offset = 0L

  def sizeOf(v: Any): Long = v match {
    case _: Int => 4L
    case _: Long => 8L
    case _: String => v.asInstanceOf[String].length + 1
    case tuple@(_: Any, _: Any) => sizeOf(tuple._1) + sizeOf(tuple._2)
  }

  def store[T](v: T): Long = v match {
    case tuple@(_: Any, _: Any) => {
      val ret = store(tuple._1)
      store(tuple._2)
      ret
    }
    case _ => {
      val ret = offset
      offset += pmemory.store(offset, v)
      ret
    }
  }

  def load[K: ClassTag, V: ClassTag](_offset: Long): (K, V) = {
    val k = load[K](_offset)
    val sizek = k match {
      case _: Int => 4L
      case _: Long => 8L
      case _: String => k.asInstanceOf[String].length + 1
    }
    val v = load[V](_offset + sizek)
    (k, v)
  }

  def load[T: ClassTag](_offset: Long): T = pmemory.load[T](_offset)

  def flush = {
    pmemory.store(0L, offset)
    pmemory.flush
  }

  if(_init) {
    if( !pmemory.trim(0L, pmemory.size) ) {
      println("trim not supported")
    }
    offset += pmemory.store(0L, 8L)
  } else {
    offset = load[Long](0)
  }

  override def finalize:Unit = {
    pmemory.store(0L, offset)
  }
}
