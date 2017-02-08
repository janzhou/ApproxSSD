package org.janzhou.nvmr.pmemory

/**
  * Created by jan on 11/5/15.
  */
object PlogSetup {
  def main (args: Array[String]) {
    val plog = new Plog(args(0), true)
  }
}
