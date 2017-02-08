package org.janzhou.nvmr.executor

import akka.actor.{ActorSystem, Props}
import akka.actor.{ActorLogging, Actor, ActorRef}
import scala.reflect.ClassTag

import org.janzhou.nvmr.partition._
import org.janzhou.nvmr.storage._

import scala.concurrent.{ExecutionContext, Await, Future}
import java.util.concurrent.Executors
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import java.util.concurrent.Semaphore

import org.janzhou.nvmr.console
import org.janzhou.nvmr.config

import scala.language.postfixOps

case class ExecutorCompute[T:ClassTag](p: Partition[T])
case class ExecutorPersist[T:ClassTag](p: Partition[T])
case class ExecutorCache[T:ClassTag](p: Partition[T])
case class ExecutorGC[T:ClassTag](p: Partition[T])
case class ExecutorReleaseLock(lock: ExecutorLock)
case object ExecutorDone

class ExecutorLock {
  private val semaphore = new Semaphore(1)
  semaphore.drainPermits()

  def acquire = {
    semaphore.acquire()
  }

  def release = {
    semaphore.release()
  }
}

class Executor(drive:Int) extends Actor with ActorLogging {
  def receive = {
    case ExecutorReleaseLock(lock) =>
      lock.release
    case ExecutorCompute(p) =>
      p.compute
    case ExecutorPersist(p) =>
      p.persist(drive)
    case ExecutorCache(p) =>
      p.cache
    case ExecutorDone =>
      sender ! "Done"
      context stop self
  }
}

object Executor {
  private val system = ActorSystem("nvmr")

  private var _instance:List[ActorRef] = null
  def instance = {
    if( _instance == null ) {
      var index = 0
      _instance = Storage.logs.map(i => {
        val drive = index
        val actor = system.actorOf(Props(new Executor(drive)), "executor-" + drive)
        index += 1
        actor
      })
    }
    _instance
  }

  def releaseLock(lock: ExecutorLock, index:Int) = {
    instance(index) ! ExecutorReleaseLock(lock)
  }

  def cache[T:ClassTag](pl: Array[Partition[T]]) = {
    var index = 0
    pl.foreach ( p => {instance(index % instance.length) ! ExecutorCache(p); index += 1} )
  }

  def persist[T:ClassTag](pl: Array[Partition[T]]) = {
    var index = 0
    pl.foreach ( p => {instance(index % instance.length) ! ExecutorPersist(p); index += 1} )
  }

  private val cores = Runtime.getRuntime().availableProcessors();

  def compute[T:ClassTag](pl: Array[Partition[T]]) = {
    val pool = Executors.newFixedThreadPool(4 * cores)
    implicit val ec = ExecutionContext.fromExecutor(pool) // ExecutionContext.global
    implicit val timeout = Timeout(1 hours) //5 seconds)

    def r(p:Partition[T]): Future[Partition[T]] = Future {
      p.compute
      p
    }

    val tasks = pl.map( r(_) ).toSeq
    Await.result(Future.sequence(tasks), timeout.duration)
    pool.shutdown
  }

  private var _gc_submitted = 0L
  private var _gc_done = 0L

  def gc_wait:Long = {
    _gc_submitted - _gc_done
  }

  def gc_done(n:Int = 1) = {
    this.synchronized {
      _gc_done += n
    }
  }

  def gc_submit(n:Int = 1)= {
    this.synchronized {
      _gc_submitted += n
    }
  }

  def compute[T:ClassTag](pl: Array[Partition[T]], index:Int) = {
    assert( 0 <= index )
    assert( index < instance.length )

    for ( p <- pl ) {
      instance(index) ! ExecutorCompute(p)
    }
  }

  def gc[T:ClassTag](pl: Array[Partition[T]]) = {
    gc_submit(pl.length)
    for ( index <- 0 to (pl.length - 1) ) {
      instance(index % instance.length) ! ExecutorGC(pl(index))
    }
  }

  private val runtime = Runtime.getRuntime()
  def freeMemory:Float = {
    val usedMemory = runtime.totalMemory() - runtime.freeMemory()
    val maxMemory  = runtime.maxMemory()
    (maxMemory - usedMemory).toFloat / maxMemory.toFloat
  }

  private def _gc = {
    runtime.gc
  }

  def gc:Boolean = {
    console.debug("freeMemory " + freeMemory)
    console.debug("gc sumitted: " + _gc_submitted + "; gc finished: " + _gc_done + "; gc wait: " + (_gc_submitted - _gc_done))
    val free = freeMemory
    if ( free < config.getDouble("executor.gc_threshold") ) {
      console.debug("Memory Full. Wait GC.")
      Thread.sleep(config.getInt("executor.sleep"))
      if( config.getBoolean("executor.gc_force") &&
        gc_wait < config.getLong("executor.gc_wait") ){
          console.debug("Force GC")
          _gc
      }
      true
    } else false
  }

  def close() = {
    for ( actor <- instance ) {
      implicit val timeout = Timeout(1 hours)
      val future = actor ? ExecutorDone
      Await.result(future, timeout.duration)
    }
    gc
    system.terminate()
    for ( log <- Storage.logs ) {
      log.close
    }
  }

  override def finalize = close()
}
