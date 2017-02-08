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
import akka.routing.{RoundRobinGroup, TailChoppingGroup}
import scala.concurrent.duration._
import java.util.concurrent.Semaphore

import org.janzhou.nvmr.console
import org.janzhou.nvmr.config

import scala.language.postfixOps

object NewExecutor {
  private val cores = Runtime.getRuntime().availableProcessors();
  private val computeActorSystem = ActorSystem("compute",
    config.getConfig("akka"))
  private val storageActorSystem = ActorSystem("storage",
    config.getConfig("akka"))

  lazy val computeActor = {
    for( i <- 0 to cores - 1 ) yield {
      computeActorSystem.actorOf(
        Props(new ComputeExecutor(i)).withDispatcher("compute-mailbox"),
        "compute-" + i)
    }
  }

  val computeRouter = {
    val paths = computeActor.map( _.path.toString )
    computeActorSystem.actorOf(
      RoundRobinGroup(paths).props().withDispatcher("compute-mailbox"),
     "computeRouter"
    )
  }

  lazy val cacheActor = {
    for( i <- 0 to Storage.logs.length - 1 ) yield {
      storageActorSystem.actorOf(
        Props(new StorageExecutor(i)).withMailbox("cache-mailbox"),
        "cache-" + i)
    }
  }

  lazy val persistActor = {
    for( i <- 0 to Storage.logs.length - 1 ) yield {
      storageActorSystem.actorOf(
        Props(new StorageExecutor(i)).withDispatcher("persist-mailbox"),
        "persist-" + i)
    }
  }

  val persistRouter = {
    val paths = persistActor.map( _.path.toString )
    storageActorSystem.actorOf(RoundRobinGroup(paths).props(), "persistRouter")
  }

  def cache[T:ClassTag](p: Partition[T]) = {
    cacheActor(p.device) ! Action.cache(p)
  }

  def persist[T:ClassTag](p: Partition[T]) = {
    persistRouter ! Action.persist(p)
  }

  def compute[T:ClassTag](p: Partition[T]) = {
    computeRouter ! Action.compute(p)
  }

  def action[T:ClassTag](action:Action.action[T]) = {
    action match {
      case action:Action.cache[T]   => cacheActor(action.p.device) ! action
      case action:Action.compute[T] => computeRouter ! action
      case action:Action.persist[T] => persistRouter ! action
    }
  }

  private val runtime = Runtime.getRuntime()
  def freeMemory:Float = {
    val usedMemory = runtime.totalMemory() - runtime.freeMemory()
    val maxMemory  = runtime.maxMemory()
    (maxMemory - usedMemory).toFloat / maxMemory.toFloat
  }

  def close() = {
    storageActorSystem.terminate()
    computeActorSystem.terminate()
    for ( log <- Storage.logs ) {
      log.close
    }
  }

  override def finalize = close()
}
