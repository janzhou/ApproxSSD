package org.janzhou.nvmr.partition

import scala.reflect.ClassTag

import org.janzhou.nvmr.pmemory._

abstract class MapPartition[T:ClassTag, U:ClassTag] extends Partition[(T, U)] {

  def mapValues[V:ClassTag](f:(U => V)):MapPartition[T, V] = {
    new MapValuesPartition(this, f);
  }

  def reduceByKey(f: (U, U) => U):ReduceByKeyPartition[T, U] = {
    new ReduceByKeyPartition(this, f)
  }
}
