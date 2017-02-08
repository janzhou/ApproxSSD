package org.janzhou.nvmr

import org.janzhou.nvmr.storage._
import org.janzhou.nvmr.ldd._

object StorageTest {
  def main (args: Array[String]) {
    val device = args(0)
    val storage = new Driver(device)
    val content = "Hello world"

    println(storage.writeObject(0, content))

    val new_content = storage.readObject[String](0, Serializer.serialize(content).limit())

    println(new_content)
  }
}
