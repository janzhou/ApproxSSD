package org.janzhou.nvmr.partition

import scala.reflect.ClassTag
import scala.collection.GenTraversableOnce

import org.janzhou.nvmr.pmemory._
import org.janzhou.nvmr.console

class FlatMapPartition[T:ClassTag, U:ClassTag](
  @transient private var p:Partition[U],
  @transient private var f:(U => GenTraversableOnce[T])) extends Partition[T] {

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
    console.debug("flatMap " + id)
    val cache = p.content.flatMap(f)
    p = null
    f = null
    cache
  }
}
