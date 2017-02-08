package org.janzhou.nvmr.ldd

import scala.reflect.ClassTag
import org.janzhou.nvmr.ldd._
import org.janzhou.nvmr.partition._
import org.janzhou.nvmr.executor._
import org.janzhou.nvmr._

/** Convert LDD[U] to LDD[T] */
class MapPartitionsLDD[T: ClassTag, U:ClassTag](
  @transient private var p:LDD[U],
  @transient private var f:(Partition[U] => Partition[T])
  ) extends LDD[T] {
  
  override def getPartitions = {
    val partitions = p.partitions.map(f)
    p = null // do not need parent ldd any more
    f = null
    partitions
  }

}
