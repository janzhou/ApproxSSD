package org.janzhou.nvmr

import org.janzhou.nvmr._
import org.janzhou.nvmr.partition._
import org.janzhou.nvmr.executor._
import org.janzhou.nvmr.ldd._

object ExecutorTest {
  def main (args: Array[String]) {
    var t = new TextFileLDD("/home/jan/data/enwikisource-20150901-pages-meta-history.10000")

    val sample = t.flatMap(line => line.replace(']', ' ')
                                 .replace('[', ' ')
                                 .replace('{', ' ')
                                 .replace('}', ' ')
                                 .replace('(', ' ')
                                 .replace(')', ' ')
                                 .replace(',', ' ')
                                 .replace('.', ' ')
                                 .replace(':', ' ')
                                 .replace('_', ' ')
                                 .replace('/', ' ')
                                 .replace('=', ' ')
                                 .replace('>', ' ')
                                 .replace('<', ' ')
                                 .replace('\n', ' ')
                                 .replace('*', ' ')
                                 .replace('"', ' ')
                                 .replace('!', ' ')
                                 .replace('+', ' ')
                                 .replace('-', ' ')
                                 .replace('&', ' ')
                                 .replace('@', ' ')
                                 .replace(';', ' ')
                                 .replace('*', ' ')
                                 .replace('|', ' ')
           .split(" "))//.filter(_.length > 0).balance().toMapLDD(m => (m, 1)).reduceByKey( _ + _ ).sortWithPartitions( _._1 < _._1 )

    t = null
    println(Executor.freeMemory)
    sample.compute
    println("samplePartitions computed")
    println(Executor.freeMemory)

    Executor.finalize
    Executor.gc
    println(Executor.freeMemory)
  }
}
