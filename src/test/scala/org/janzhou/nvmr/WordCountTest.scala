package org.janzhou.nvmr

import org.janzhou.nvmr._
import org.janzhou.nvmr.partition._
import org.janzhou.nvmr.executor._
import org.janzhou.nvmr.ldd._

import java.time.{Instant, Duration}

object WordCountTest {
  def textFile(dir:String):TextFileLDD = {
    new TextFileLDD(dir)
  }
  def main (args: Array[String]) {
    val file = args(0)
    val sample =
      textFile(file)
      .flatMap(line => line.split(" "))
      // .flatMap(line => line.replace(']', ' ')
                            // .replace('[', ' ')
                            // .replace('{', ' ')
                            // .replace('}', ' ')
                            // .replace('(', ' ')
                            // .replace(')', ' ')
                            // .replace(',', ' ')
                            // .replace('.', ' ')
                            // .replace(':', ' ')
                            // .replace('_', ' ')
                            // .replace('/', ' ')
                            // .replace('=', ' ')
                            // .replace('>', ' ')
                            // .replace('<', ' ')
                            // .replace('\n', ' ')
                            // .replace('*', ' ')
                            // .replace('"', ' ')
                            // .replace('!', ' ')
                            // .replace('+', ' ')
                            // .replace('-', ' ')
                            // .replace('&', ' ')
                            // .replace('@', ' ')
                            // .replace(';', ' ')
                            // .replace('*', ' ')
                            // .replace('|', ' ')
           // .split(" "))
           //.toMapLDD(m => (m, 1)).reduceByKey( _ + _ )
    // val results = sample.sortWithPartitions( _._1 < _._1 )

    Executor.gc
    sample.persist
    val start = Instant.now()
    sample.compute
    Executor.finalize
    val end = Instant.now()
    Executor.gc

    println(Duration.between(start, end))
  }
}
