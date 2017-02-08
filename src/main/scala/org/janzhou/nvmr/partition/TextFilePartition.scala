package org.janzhou.nvmr.partition

import scala.reflect.ClassTag
import scala.io.Source
import scala.io.Codec
import org.janzhou.nvmr._
import org.janzhou.nvmr.pmemory._
import org.janzhou.nvmr.storage.Driver
import java.io.ByteArrayInputStream
import java.nio.charset.CodingErrorAction
import java.util.concurrent.Semaphore

import org.janzhou.nvmr.storage._

object TextFilePartition {
  val sync = config.getBoolean("TextFileLDD.sync")
}

class TextFilePartition(loc:(String, Long, Long)) extends Partition[String] {
  private val file:String = loc._1
  private val offset:Long = loc._2
  private val size:Long = loc._3

  private var TextFileCache: Array[String] = null
  override def computed:Boolean = true
  override def cached:Boolean = {
    TextFileCache != null
  }

  override def device:Int = ( id % Storage.logs.length ).toInt

  override def depends:Set[Partition[_]] = {
    if( cached ) {
      Set[Partition[_]]()
    } else {
      Set[Partition[_]](this)
    }
  }

  private var _lock:Semaphore = null
  override def setLock(lock:Semaphore) = {
    this.synchronized{
      if( cached ) {
        if( _lock != null ) {
          _lock.release()
        }
        _lock = lock
        if( _lock != null ) {
          _lock.acquire()
        }
      } else {
        _lock = lock
      }
    }
  }

  override def cache = {
    this.synchronized{
      if( !cached ) {
        if( _lock != null ) _lock.acquire()
        TextFileCache = one
      }
    }
  }

  override def persist(implicit drive:Int = 0) = {
    this.synchronized{
      if( cached ) {
        if( _lock != null ) _lock.release()
        TextFileCache = null
      }
    }
  }

  override protected def finalize = {
    persist(0)
  }

  override def content: Array[String] = {
    if( cached ) {
      TextFileCache
    } else {
      one
    }
  }

  override def one = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    console.log("Load TextFilePartition " + id)

    if( offset == -1L ) {
      Source.fromFile(file).getLines.toArray
    } else {
      val driver = new Driver(file)
      val buf = {
        if( TextFilePartition.sync ) {
          TextFilePartition.synchronized {
            driver.read(offset, size)
          }
        } else {
          driver.read(offset, size)
        }
      }
      val input = new ByteArrayInputStream(buf.array())
      Source.fromInputStream(input).getLines.toArray
    }
  }
}
