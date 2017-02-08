package org.janzhou.nvmr.partition

import scala.reflect.ClassTag

import org.janzhou.nvmr.pmemory._

class GroupByPartition[T:ClassTag, U:ClassTag](
  @transient private var p:Partition[U],
  @transient private var f:(U => T)) extends MapPartition[T, Array[U]] {

  override def one = {
    val cache = p.content.groupBy(f).toArray
    p = null
    f = null
    cache
  }
}
