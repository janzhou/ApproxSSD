package org.janzhou.nvmr.partition

import scala.reflect.ClassTag

import org.janzhou.nvmr.pmemory._
import org.janzhou.nvmr.console

class MapFunctionPartition[T:ClassTag, U:ClassTag](
  @transient private var p:Partition[U],
  @transient private var f:(U => T)) extends Partition[T] {

  override def depends:Set[Partition[_]] = {
    if( computed ) {
      if( cached ) {
        Set[Partition[_]]()
      } else {
        Set[Partition[_]](this)
      }
    } else {
      p.depends
    }
  }

  override def one = {
    val cache = (p.content.map(f))
    p = null
    f = null
    cache
  }
}
