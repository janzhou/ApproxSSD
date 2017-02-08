package org.janzhou.nvmr

import org.janzhou.nvmr.pmemory._

import scala.reflect.ClassTag

/**
  * Created by jan on 11/6/15.
  */
object TupleTest {
  def main (args: Array[String]) {
    val hello = "Hello"
    val world = "World"

    val offset = memory.plog.store((hello, hello.length))
    println(memory.plog.load[String, Int](offset))

    val parray = new Pkvarray[String, Int]
    parray.set(Array((hello, hello.length), (world, world.length)))
    parray.get.map(println)
  }
}
