package org.janzhou.nvmr.executor

import akka.actor.{ActorSystem, ActorLogging, Actor, ActorRef}
import org.janzhou.nvmr.partition._
import scala.reflect.ClassTag

import akka.dispatch.PriorityGenerator
import akka.dispatch.UnboundedStablePriorityMailbox
import com.typesafe.config.Config

import org.janzhou.nvmr.storage._

class StorageMailbox(settings: ActorSystem.Settings, config: Config)
extends UnboundedStablePriorityMailbox(
  PriorityGenerator {
    case Action.persist(p) => 0
    case Action.cache(p) => 1
    case _ => 2
  }
)

class StorageExecutor(drive:Int) extends Actor with ActorLogging {
  import Action._

  def receive = {
    case cache(p) =>
      p.cache
      p.next()
    case persist(p) =>
      p.persist(drive)
      p.next()
  }
}
