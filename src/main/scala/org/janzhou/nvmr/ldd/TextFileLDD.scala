package org.janzhou.nvmr.ldd

import org.janzhou.nvmr.ldd._
import org.janzhou.nvmr.partition._
import org.janzhou.nvmr.executor._
import org.janzhou.nvmr._
import java.io.File

class TextFileLDD (dir:String) extends LDD[String] {
  private def getFiles(dir: String):Array[(String, Long, Long)] = {
    val d = new File(dir)
    val size = config.getLong("TextFileLDD.size")
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toArray.map(f =>
        (dir + "/" + f.getName().toString(), -1L, -1L)
      )
    } else if (d.exists && d.isFile) {
      var offset = 0L
      var array = Array[(String, Long, Long)]()
      while ( offset < d.length ) {
        val _offset = offset
        var _size = size

        offset = _offset + _size

        if( offset > d.length ) {
          _size = d.length - _offset
        }

        array = array :+ (dir, _offset, _size)
      }
      array
    } else {
      Array[(String, Long, Long)]()
    }
  }

  console.log("TextFileLDD " + getFiles(dir).length + " partitions")

  override protected def getPartitions: Array[Partition[String]] = {
    getFiles(dir).map(f => new TextFilePartition(f))
  }
}
