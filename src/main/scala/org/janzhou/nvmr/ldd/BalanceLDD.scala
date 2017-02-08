package org.janzhou.nvmr.ldd

import scala.reflect.ClassTag
import org.janzhou.nvmr.ldd._
import org.janzhou.nvmr.partition._
import org.janzhou.nvmr._

class BalanceLDD[T: ClassTag](
  @transient private val _p:LDD[T]
  ) extends LDD[T] {

  @transient private val p:LDD[T] = _p

  override def getPartitions = {
    p.compute
    p.partitions.filter(_.content.length != 0)
  }

}
