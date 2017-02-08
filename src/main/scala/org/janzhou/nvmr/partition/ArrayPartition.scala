package org.janzhou.nvmr.partition

import scala.reflect.ClassTag

import org.janzhou.nvmr.pmemory._

class ArrayPartition[T:ClassTag](@transient private var a:Array[T]) extends Partition[T] {
  override def one = {
    val cache = a
    a = null
    cache
  }
}
