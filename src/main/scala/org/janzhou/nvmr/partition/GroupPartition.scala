package org.janzhou.nvmr.partition

import scala.reflect.ClassTag

import org.janzhou.nvmr.pmemory._
import org.janzhou.nvmr.ldd._
import org.janzhou.nvmr.console

/** Group a list of Partitions. */
class GroupPartition[T:ClassTag, U:ClassTag](
  @transient private var group:Array[Partition[(T, U)]] = null
  ) extends MapPartition[T, U] {

  override def one = {
    val contents = group.map( _.content ).reduce( _ ++ _)
    group = null
    contents
  }
}
