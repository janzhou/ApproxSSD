package org.janzhou.nvmr.partition

import scala.reflect.ClassTag

import org.janzhou.nvmr.pmemory._
import org.janzhou.nvmr.console

class toMapPartition[T:ClassTag, U:ClassTag, V:ClassTag](
  @transient private var p:Partition[T],
  @transient private var f: T => (U, V)) extends MapPartition[U, V] {

  override def one = {
    val cache = (p.content.map(f))
    p = null
    f = null
    cache
  }
}
