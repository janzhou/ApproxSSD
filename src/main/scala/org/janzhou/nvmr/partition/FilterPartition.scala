package org.janzhou.nvmr.partition

import scala.reflect.ClassTag

import org.janzhou.nvmr.pmemory._

class FilterPartition[T:ClassTag](
  @transient private var p:Partition[T],
  @transient private var f:(T => Boolean)) extends Partition[T] {

  override def one = {
    val cache = p.content.filter(f)
    p = null
    f = null
    cache
  }
}
