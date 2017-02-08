package org.janzhou.nvmr.ldd

import scala.reflect.ClassTag
import org.janzhou.nvmr.ldd._
import org.janzhou.nvmr.partition._
import org.janzhou.nvmr._

class ArrayMapLDD[T:ClassTag, U: ClassTag, V:ClassTag](_p:LDD[T] = null, f: T => (U, V)) extends MapLDD[U, V] {
  if( _p!= null) {
    _partitions = _p.partitions.map(_.toMapPartition(f))
  }
}
