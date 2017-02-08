package org.janzhou.nvmr

import java.io.File
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import collection.JavaConversions._

object config {
  private val _config = {
    val _default = ConfigFactory.load("default")
    val _fallback = _default.getStringList("Config.Fallback").map(
      file => new File(file)
    ).filter( file => file.exists() && file.isFile() ).map( file => {
      val parseFile = ConfigFactory.parseFile(file)
      ConfigFactory.load(parseFile)
    })

    if( _fallback.length > 0 ) {
      _fallback.reduce( _.withFallback(_) ).withFallback( _default )
    } else {
      _default
    }
  }

  def getBoolean(_c:String):Boolean = {
    _config.getBoolean(_c)
  }

  def getString(_c:String):String = {
    _config.getString(_c)
  }

  def getStringList(_c:String):List[String] = {
    _config.getStringList(_c).toList
  }

  def getInt(_c:String):Int = {
    _config.getInt(_c)
  }

  def getLong(_c:String):Long= {
    _config.getLong(_c)
  }

  def getDouble(_c:String):Double = {
    _config.getDouble(_c)
  }

  def getConfig(_c:String):Config = {
    _config.getConfig(_c)
  }
}
