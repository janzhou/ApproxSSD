package org.janzhou.nvmr.pmemory

import java.io.File
import com.typesafe.config.ConfigFactory
import collection.JavaConversions._

/**
  * Created by jan on 11/6/15.
  */
object memory {
  private val file = new File("src/main/resources/applications.conf")
  private val config = {
    if( file.exists() && file.isFile() ) {
      val parseFile = ConfigFactory.parseFile(file)
      ConfigFactory.load(parseFile)
    } else {
      ConfigFactory.load()
    }
  }
  private val workdir = config.getString("pmemory.workdir")
  val plogs = config.getStringList("pmemory.devices").toList.map(dev => new Plog(workdir + "/" + dev))

  val plog = plogs(0)
  val logs = plogs
}
