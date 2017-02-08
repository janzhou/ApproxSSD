package org.janzhou.nvmr

import org.janzhou.nvmr._
import org.janzhou.nvmr.ldd._
import org.janzhou.nvmr.storage._

object StorageTrimTest {
  def main (args: Array[String]) {
    val device = args(0)
    val driver = new Driver(device)

    if ( driver.trim(0, 1024*8) == -1 ) {
      println("BLKDISCARD ioctl failed")
    }
  }
}
