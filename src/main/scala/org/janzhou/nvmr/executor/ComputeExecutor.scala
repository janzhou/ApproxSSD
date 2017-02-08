package org.janzhou.nvmr.executor

import akka.actor.{ActorSystem, ActorLogging, Actor, ActorRef}
import org.janzhou.nvmr.partition._
import scala.reflect.ClassTag

import akka.dispatch.PriorityGenerator
import akka.dispatch.UnboundedStablePriorityMailbox
import com.typesafe.config.Config

import org.janzhou.nvmr.console

class ComputeExecutor(core:Int) extends Actor with ActorLogging {
  import Action._

  def receive = {
    case compute(p) =>
      p.compute
      p.next()
  }
}
