package org.janzhou.nvmr.ldd

import scala.reflect.ClassTag
import org.janzhou.nvmr.ldd._
import org.janzhou.nvmr.partition._
import org.janzhou.nvmr._

class GroupByPartitionsLDD[T:ClassTag, U:ClassTag](
  @transient private val _p:LDD[U],
  @transient private val _f:(U => T)) extends MapLDD[T, Array[U]] {
  @transient private var p = _p
  @transient private var f = _f

  override def getPartitions = p.partitions.map(p => p.groupBy(f));
}
