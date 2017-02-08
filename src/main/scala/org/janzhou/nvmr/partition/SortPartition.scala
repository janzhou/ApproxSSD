package org.janzhou.nvmr.partition

import scala.reflect.ClassTag

import org.janzhou.nvmr.pmemory._

class SortPartition[T:ClassTag](
  @transient private var p:Partition[T],
  @transient private var f:((T, T) => Boolean)) extends Partition[T] {

  override def one = {
    val cache = p.content.sortWith(f)
    p = null
    f = null
    cache
  }
}
