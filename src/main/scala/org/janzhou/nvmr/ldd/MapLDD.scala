package org.janzhou.nvmr.ldd

import scala.reflect.ClassTag
import org.janzhou.nvmr.ldd._
import org.janzhou.nvmr.partition._
import org.janzhou.nvmr._

/** Abstract key-value LDD */
abstract class MapLDD[T: ClassTag, U: ClassTag] extends LDD[(T, U)] {
  def getKeys() = {
    this.map( _._1 )
  }

  /** Run Reduce on each Partitions. */
  def reduceByKeyPartitions(f:(U, U) => U):MapLDD[T, U] = {
    new MapPartitionsLDD[(T, U) , (T, U)](this,
      _.toMapPartition(v => (v._1, v._2)).reduceByKey(f)
    ).toMapLDD(v => (v._1, v._2))
  }

  def reduceByKey(f: (U, U) => U):ReduceByKeyLDD[T, U] = {
    new ReduceByKeyLDD(this, f)
  }
}
