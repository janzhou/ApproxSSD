package org.janzhou.nvmr

import org.janzhou.nvmr.storage._
import org.janzhou.nvmr.ldd._

object DeviceTest {
  def main (args: Array[String]) {
    val device = new Device("/tmp/nvmr")
    val content = "Hello world"
    val long = 0L

    val loc = device.store(long)

    println(device.load[String](device.store(content)))
    println(device.load[Long](loc))
    println(loc)
  }
}
