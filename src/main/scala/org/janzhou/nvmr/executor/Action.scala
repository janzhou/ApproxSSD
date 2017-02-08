package org.janzhou.nvmr.executor

import org.janzhou.nvmr.partition._
import scala.reflect.ClassTag

object Action {
  abstract class action[T:ClassTag](p: Partition[T])
  case class cache[T:ClassTag](p: Partition[T]) extends action(p)
  case class persist[T:ClassTag](p: Partition[T]) extends action(p)
  case class compute[T:ClassTag](p: Partition[T]) extends action(p)
}
