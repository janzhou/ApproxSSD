package org.janzhou.nvmr

import org.janzhou.nvmr.storage._
import java.nio.ByteBuffer

class BenchmarkThread(id:Int, count:Int, device:String) extends Thread {
  val storage = new Driver(device)
  val pagesize = 1024*8
  val buffer = ByteBuffer.allocate(1024*8) // one page

  val offset = 1024*1024*1024*id

  override def run() {
    for( i <- 0 to count ) {
      storage.write(offset+pagesize*i, buffer)
    }
  }
}

object StorageBenchmark {
  def main (args: Array[String]) {
    val device = args(0)
    val list = List.range(1,2)
    val thread_list = list.map( x => new BenchmarkThread(x, 1024*1024, device) )
    thread_list.foreach(thread => thread.start())
    thread_list.foreach(thread => thread.join())
    val thread = new BenchmarkThread(0, 1024, device)
    thread.start()
    thread.join()
  }
}
