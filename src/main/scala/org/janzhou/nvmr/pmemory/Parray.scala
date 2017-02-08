package org.janzhou.nvmr.pmemory

import scala.reflect.ClassTag

/**
  * Created by jan on 11/5/15.
  */
class Parray[T:ClassTag](log:Int = 0) extends Serializable {
  private var _offset = 0L
  private var _length = 0

  @transient
  private def plog = {
      memory.plogs(log)
  }

  def offset = _offset

  def store(a:Array[T]):Unit = {
    require(_offset == 0, "parray immutable")
    val offset = a.map(v => plog.store(v))
    _offset = offset(0)
    _length = a.length
  }

  def load:Array[T] = {
    val seed = (0 to _length - 1).toArray
    var offset = _offset
    seed.map(v => {
      val v = plog.load[T](offset)
      offset = offset + plog.sizeOf(v)
      v
    })
  }

  @deprecated("Use store instead", "01-29-2016")
  def set = store _

  @deprecated("Use load instead", "01-29-2016")
  def get = { load }
}
