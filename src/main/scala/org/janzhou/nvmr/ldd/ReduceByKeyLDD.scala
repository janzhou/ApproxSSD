package org.janzhou.nvmr.ldd

import scala.reflect.ClassTag
import org.janzhou.nvmr.ldd._
import org.janzhou.nvmr.partition._
import org.janzhou.nvmr._
import org.janzhou.nvmr.executor._

class ReduceByKeyLDD[T: ClassTag, U:ClassTag](
  @transient private var _p:MapLDD[T, U],
  @transient private var f: (U, U) => U
  ) extends MapLDD[T, U] {

  val p:MapLDD[T, U] = _p.reduceByKeyPartitions(f)
  _p = null

  override protected def persist_compute = {
    println("ReduceByKeyLDD persist compute")
    val partition_slides = partitions.sliding(64, 64).toArray
    val parrent_slides   = p.partitions.sliding(128, 128).toArray

    for ( i <- 0 to (partition_slides.length - 1) ) {
      console.log("Reduce Slide " + ( i + 1 ) + "/" + partition_slides.length)
      for ( slides <- parrent_slides ) {
        Executor.cache(slides)
        partition_slides(i).foreach( p => {
          p.setReduceParrent(new ArrayLDD(slides))
        })
        Executor.compute(partition_slides(i))
        Executor.gc(slides)
      }
      Executor.gc(partition_slides(i))
    }
  }

  override def getPartitions = {
    if( _persist ) p.persist

    val sample = p.samplePartitions(config.getInt("ReduceByKeyLDD.samplePartitions"))
    sample.persist
    val s = sample.sample(config.getInt("ReduceByKeyLDD.samplePerPartition"))
    .map(_._1)
    s.compute

    def compare(a:T, b:T) = (a, b) match {
      case (a:Int, b:Int) => a < b
      case (a:Long, b:Long) => a < b
      case (a:String, b:String) => a < b
      case _ => a.toString < b.toString
    }
    val a = s.mergePartitions.sortWith( compare )

    val c = a.content.distinct

    val ranges = for (i <- 0 to c.length - 2) yield {
      (c(i), c(i+1))
    }

    def between(a:T, b:T, c:T):Boolean = (a, b, c) match {
      case (a:Int, c:Int, b:Int) => a <= b && b < c
      case _ => a.toString <= b.toString && b.toString < c.toString
    }

    ranges.toArray.map( r => {
      def filter(v:(T, U)):Boolean = between(r._1, v._1, r._2)
      new ReducePartition(p, filter, f)
    }) :+ ( {
      def filter(v:(T, U)):Boolean = !between(c(0), v._1, c(c.length -1))
      new ReducePartition(p, filter, f)
    } )
  }

}
