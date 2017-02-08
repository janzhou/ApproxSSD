package org.janzhou.nvmr.partition

import scala.reflect.ClassTag

import org.janzhou.nvmr.pmemory._
import org.janzhou.nvmr.ldd._
import org.janzhou.nvmr.console

/** Reduce a list of Partitions. */
class ReducePartition[T:ClassTag, U:ClassTag](
  @transient private var p:LDD[(T, U)] = null,
  @transient private var filter: ((T, U)) => Boolean,
  @transient private var f: (U, U) => U ) extends MapPartition[T, U] {

  override def setReduceParrent(_p:LDD[(T,U)]) = {
    p = _p
  }

  override def one = {
    val contents = p.filter(filter).partitions.map(_.content).reduce(_ ++ _)
    val _cache = {
      if( contents.length == 0 ) {
        val parrent = new ArrayPartition( contents )
                      .toMapPartition(v => (v._1, v._2))
        parrent.reduceByKey(f).content
      } else {
        null
      }
    }
    p = null
    filter = null
    f = null
    _cache
  }
}
