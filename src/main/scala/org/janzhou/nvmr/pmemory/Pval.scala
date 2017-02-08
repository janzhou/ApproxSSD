package org.janzhou.nvmr.pmemory

import scala.reflect.ClassTag

/**
  * Created by jan on 11/5/15.
  */
class Pval[T:ClassTag]( plog:Plog ) {
  private var _offset = 0L

  def set(v:T):Unit = {
    require(_offset == 0, "pval immutable")
    _offset = plog.store(v)
  }

  def get:T = {
    plog.load[T](offset)
  }

  def offset = _offset
}
