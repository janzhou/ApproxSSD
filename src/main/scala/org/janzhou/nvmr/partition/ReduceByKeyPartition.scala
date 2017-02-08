package org.janzhou.nvmr.partition

import scala.reflect.ClassTag

import org.janzhou.nvmr.pmemory._

/** Run Reduce on one Partition. */
class ReduceByKeyPartition[T:ClassTag, U:ClassTag](
  @transient private var p:MapPartition[T, U],
  @transient private var f: (U, U) => U ) extends MapPartition[T, U] {

  override def one = {
    val cache = p.content.groupBy( k => k._1 )
                 .toArray.map(v => (v._1, v._2.map(_._2).reduce(f)))
    p = null
    f = null
    cache
  }
}
