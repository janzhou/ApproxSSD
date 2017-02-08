package org.janzhou.nvmr.storage

import scala.reflect.ClassTag
import java.util.concurrent.Semaphore
import org.janzhou.nvmr.config

object Block {
  val cache_slots = new Semaphore(config.getInt("storage.cache_slots"))

  private var cache_blocks = Set[Block[_]]()
  private var persist_blocks = Set[Block[_]]()

  def add(block:Block[_]) = {
    this.synchronized {
      if ( block.cached ) {
        cache_blocks += block
      } else {
        persist_blocks += block
      }
    }
  }

  def remove(block:Block[_]) = {
    this.synchronized {
      if ( block.cached ) {
        cache_blocks -= block
      } else {
        persist_blocks -= block
      }
    }
  }
}

class Block[T:ClassTag]( private var _cache:Array[T] = null ) {
  //private var _cache:Array[T] = null
  private var _persist:(Long, Long) = null
  private var _lock:Semaphore = null

  Block.add(this)

  var drive:Int = 0
  def device = drive
  private def engine = {
    Storage.logs(drive)
  }

  final def setLock(lock:Semaphore) = {
    this.synchronized {
      if ( _cache != null ) {
        if( _lock != null ) _lock.release()
        _lock = lock
        if( _lock != null ) _lock.acquire()
      } else {
        _lock = lock
      }
    }
  }

  override def finalize = {
    this.synchronized {
      if( _lock != null && _cache != null ) {
        _lock.release()
      }
    }
  }

  final def persist(implicit drive:Int) = {
    this.synchronized {
      if( _lock != null ) _lock.release()
      if ( _persist == null &&
        _cache != null
      ) {
        this.drive = drive
        _persist = engine.store(_cache)
      }
      if( _cache != null ) {
        Block.remove(this)
        _cache = null
        Block.add(this)
      }
    }
  }

  final def cached:Boolean = _cache != null

  final def cache = {
    this.synchronized {
      if( _lock != null ) _lock.acquire()
      if ( _persist != null && _cache == null ) {
        Block.remove(this)
        _cache = engine.load[Array[T]](_persist)
        Block.add(this)
      }
    }
  }

  final def get:Array[T] = {
    if ( _cache == null ) {
        engine.load[Array[T]](_persist)
    } else _cache
  }

  final def set(data:Array[T]) = {
    if ( _cache == null && _persist == null ) {
      _cache = data
    }
  }

}
