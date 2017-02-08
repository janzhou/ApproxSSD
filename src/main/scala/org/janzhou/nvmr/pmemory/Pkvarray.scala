package org.janzhou.nvmr.pmemory

import scala.reflect.ClassTag

/**
  * Created by jan on 11/6/15.
  */

class Pkvarray[K:ClassTag, V:ClassTag] extends Serializable {
  private var _offset = 0L
  private var _length = 0

  @transient
  private def plog = {
    memory.plog
  }

  def offset = _offset

  def set(a:Array[(K, V)]):Unit = {
    require(_offset == 0, "parray immutable")
    val offset = a.map(v => plog.store(v))
    _offset = offset(0)
    _length = a.length
  }

  def get:Array[(K, V)] = {
    val seed = (0 to _length - 1).toArray
    var offset = _offset
    seed.map(v => {
      //val k = plog.load[K](offset)
      //offset = offset + plog.sizeOf(k)
      val v = plog.load[K, V](offset)
      offset = offset + plog.sizeOf(v)
      v
    })
  }
}
