package org.janzhou.nvmr.partition

import scala.reflect.ClassTag

import org.janzhou.nvmr.pmemory._

class MapValuesPartition[T:ClassTag, U:ClassTag, V:ClassTag](
  @transient private var p:MapPartition[T, U],
  @transient private var f:(U => V)) extends MapPartition[T, V] {

  override def one = {
    val cache = p.content.toMap.mapValues(f).toArray
    p = null
    f = null
    cache
  }
}
