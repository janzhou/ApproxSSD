package org.janzhou.nvmr.partition

import org.janzhou.nvmr.console
import org.janzhou.nvmr.executor._
import org.janzhou.nvmr.storage._
import org.janzhou.nvmr.ldd._
import scala.collection.GenTraversableOnce
import scala.reflect.ClassTag
import java.util.concurrent.Semaphore

object Partition {
  private var _id = 0L

  def get_id:Long = {
    var id = 0L
    this.synchronized {
      id = _id
      _id += 1
    }
    id
  }
}

abstract class Partition[T:ClassTag] extends Serializable {
  private var loc:(Long, Long) = null

  val id = Partition.get_id

  private var _data:Block[T]  = null
  private var _persist = false

  def device:Int = {
    if( _data == null ) {
      0
    } else {
      _data.device
    }
  }

  def depends:Set[Partition[_]] = Set[Partition[_]]()

  private var NextActions = Set[Action.action[_]]()
  def next( action:Action.action[_] = null ){
    if( action == null ) {
      if( NextActions.size > 0 ) {
        NextActions.foreach( NextAction => NewExecutor.action(NextAction) )
        NextActions =  Set[Action.action[_]]()
      }
    } else {
      NextActions += action
    }
  }

  private var _lock:Semaphore = null
  def setLock(lock:Semaphore) = {
    _lock = lock
    if ( _data != null ) {
      _data.setLock(lock)
    }
  }

  def computed:Boolean = {
    if ( _data == null ) {
      false
    } else {
      true
    }
  }

  def cached:Boolean = {
    if ( _data == null ) {
      false
    } else {
      _data.cached
    }
  }

  /** compute once */
  def one:Array[T]

  override protected def finalize = {
    Block.remove(_data)
  }

  final def compute = {
    this.synchronized {
      if ( _data == null ){
        console.debug("Compute partition " + id)
        _data = new Block(one)
        if ( _lock != null ) _data.setLock(_lock)
        if ( _persist ) persist()
      }
    }
  }

  def persist(implicit drive:Int = 0) = {
    this.synchronized {
      _persist = true
      if ( _data != null ) {
        assert( drive >= 0 )
        assert( drive < Storage.logs.length )
        _data.persist
      }
    }
  }

  def cache = {
    this.synchronized {
      compute
      _data.cache
    }
  }

  def content: Array[T] = {
    compute
    _data.get
  }

  def map[V:ClassTag](v:(T => V)):MapFunctionPartition[V, T] = {
    new MapFunctionPartition(this, v)
  }

  def flatMap[V:ClassTag](v:(T => GenTraversableOnce[V])):FlatMapPartition[V, T] = {
    new FlatMapPartition(this, v)
  }

  def groupBy[V:ClassTag](v:(T => V)):GroupByPartition[V, T] = {
    new GroupByPartition(this, v)
  }

  def filter(v:(T => Boolean)):FilterPartition[T] = {
    new FilterPartition(this, v)
  }

  def sortWith(v:((T, T) => Boolean)):SortPartition[T] = {
    new SortPartition(this, v)
  }

  def reduce(f: (T, T) => T):T = {
    content.reduce(f)
  }

  def toMapPartition[U:ClassTag, V:ClassTag](f: T => (U, V)):MapPartition[U, V] = {
    new toMapPartition(this, f)
  }

  def setReduceParrent(_p:LDD[T]):Unit = Unit

  override def toString():String = content.map(i => i.toString).reduce[String]{(a, b) => a + ", " + b}
}
