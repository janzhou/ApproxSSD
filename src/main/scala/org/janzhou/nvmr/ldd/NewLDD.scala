package org.janzhou.nvmr.ldd

import scala.util.Random
import scala.reflect.ClassTag
import scala.collection.GenTraversableOnce

import org.janzhou.nvmr._
import org.janzhou.nvmr.partition._
import org.janzhou.nvmr.executor._

abstract class NewLDD[T: ClassTag] extends LDD[T]{

  override def compute = {
  }

}
