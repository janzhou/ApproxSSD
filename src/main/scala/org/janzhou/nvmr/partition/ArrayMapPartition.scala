package org.janzhou.nvmr.partition

import scala.reflect.ClassTag

import org.janzhou.nvmr.pmemory._

class ArrayMapPartition[T:ClassTag, U:ClassTag](@transient private var a:Array[(T, U)]) extends MapPartition[T, U] {
  override def one = {
    val cache = a
    a = null
    cache
  }
}
