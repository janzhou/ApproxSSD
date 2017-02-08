/**
 * A Log-structured Distributed Dataset.
 */

package org.janzhou.nvmr.ldd

import scala.util.Random
import scala.reflect.ClassTag
import scala.collection.GenTraversableOnce

import org.janzhou.nvmr._
import org.janzhou.nvmr.partition._
import org.janzhou.nvmr.storage._
import org.janzhou.nvmr.executor._

abstract class LDD[T: ClassTag] extends Serializable {
  protected var _partitions: Array[Partition[T]] = null

  protected var _persist = false
  def persist = {
    _persist = true
  }

  private def even_split = {
    var index = 0
    var split = for ( i <- 0 to Executor.instance.length - 1 ) yield {
      val indices = i until partitions.length by Executor.instance.length
      indices.map( i => partitions(i) ).toArray
    }
    split.toArray
  }

  private def group_split = {
    val size = {
      val size = partitions.length / Executor.instance.length
      if( partitions.length % Executor.instance.length != 0 ) size + 1
      else size
    }
    partitions.grouped(size).toArray
  }

  protected def persist_compute = {
    val slides = even_split
    assert( slides.length == Executor.instance.length )
    assert( slides.map( _.length ).reduce( _ + _ ) == partitions.length )

    val locks = for ( i <- 0 to Executor.instance.length - 1 ) yield {
      new ExecutorLock()
    }

    for ( index <- 0 to slides.length - 1 ) {
      for ( p <- slides(index) ) {
        if( _persist ) p.persist(index)
      }
      Executor.compute(slides(index), index)
      Executor.releaseLock(locks(index), index)
    }

    for( lock <- locks ) {
      lock.acquire
    }
  }

  private def default_compute = {
    Executor.compute(partitions)
  }

  private def new_compute = {
    for ( partition <- partitions ) {
      val depends = partition.depends
      if( depends.size == 0 ) {
        NewExecutor.compute(partition)
      }
      for ( depend <- depends ) {
        partition.next( Action.persist(depend) )
        if( _persist ) partition.next( Action.persist(partition) )
        depend.next( Action.compute(partition) )
        depend.setLock(Block.cache_slots)
        NewExecutor.cache(depend)
      }
    }
  }

  def compute = new_compute

  def old_compute = {
    if( _persist ) {
      persist_compute
    } else {
      default_compute
    }
  }
  protected def getPartitions: Array[Partition[T]] = _partitions

  def partitions: Array[Partition[T]] = {
    if( _partitions == null ) {
      _partitions = getPartitions
    }
    _partitions
  }

  def mergePartitions:Partition[T] = {
    new ArrayPartition(partitions.map(_.content).reduce(_ ++ _))
  }

  def map[V: ClassTag](v: T => V): MapPartitionsLDD[V, T] = {
    new MapPartitionsLDD[V, T](this, p => p.map(v))
  }

  def flatMap[V:ClassTag](v:(T => GenTraversableOnce[V])):MapPartitionsLDD[V, T] = {
    new MapPartitionsLDD[V, T](this, p => p.flatMap(v))
  }

  def sample(n:Int):MapPartitionsLDD[T, T] = {
    new MapPartitionsLDD[T, T](this, p => {
      val r = new Random()
      val l = p.content.length
      val seq = for (i <- 1 to n) yield r.nextInt(l)
      new ArrayPartition(seq.distinct.map(i => p.content(i)).toArray)
    })
  }

  def samplePartitions(n:Int):LDD[T] = {
    val r = new Random()
    val l = this.partitions.length
    val seq = for (i <- 1 to n) yield r.nextInt(l)
    new ArrayLDD(seq.distinct.map(i => this.partitions(i)).toArray)
  }

  def groupByPartitions[V:ClassTag](v:(T => V)):GroupByPartitionsLDD[V, T] = {
    new GroupByPartitionsLDD(this, v)
  }

  def sortWithPartitions(v:((T, T) => Boolean)):MapPartitionsLDD[T, T] = {
    new MapPartitionsLDD(this, p => p.sortWith(v))
  }

  def balance():BalanceLDD[T] = {
    new BalanceLDD(this)
  }

  def filter(v:(T => Boolean)):MapPartitionsLDD[T, T] = {
    new MapPartitionsLDD[T, T](this, p => p.filter(v))
  }

  def toMapLDD[U:ClassTag, V:ClassTag](f: T => (U, V)):MapLDD[U, V] = {
    new ArrayMapLDD(this, f)
  }

  override def toString():String = partitions.map(p => p.toString).reduce[String]{(a, b) => a + "\n" + b}
}
