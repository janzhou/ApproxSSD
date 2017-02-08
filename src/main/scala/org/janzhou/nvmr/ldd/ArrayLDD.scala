package org.janzhou.nvmr.ldd

import scala.reflect.ClassTag
import org.janzhou.nvmr.ldd._
import org.janzhou.nvmr.partition._
import org.janzhou.nvmr._

class ArrayLDD[T: ClassTag](_p:Array[Partition[T]]) extends LDD[T] {
  if ( _p != null ) {
    add( _p )
  }
  
  def add(p:Array[Partition[T]]) = {
    if ( _partitions == null ) {
      _partitions = p
    } else {
      _partitions = _partitions ++ p
    }
  }

  def add(p:Partition[T]) = {
    if ( _partitions == null ) {
      _partitions = Array(p)
    } else {
      _partitions = _partitions :+ p
    }
  }

  def add(t:Array[T]) = {
    val p = new ArrayPartition(t)
    if ( _partitions == null ) {
      _partitions = Array(p)
    } else {
      _partitions = _partitions :+ p
    }
  }
}
