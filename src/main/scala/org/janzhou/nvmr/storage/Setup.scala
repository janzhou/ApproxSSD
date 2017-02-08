package org.janzhou.nvmr.storage

import org.janzhou.nvmr.config
import org.janzhou.nvmr.console
import java.io._
import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output

object Setup {
  val kryo = new Kryo()

  def driver_setup(dev: String) {
    val driver = new Driver(dev)
    if( 0 != driver.trim(0L, driver.size) ) {
      console.debug("trim not supported")
    }
    driver.writeObject(0L, 83L)
  }

  def device_setup(_dev: String) {
    val dev = new RandomAccessFile(_dev, "rw")
    val out = new DeviceOutputStream(dev, 0L)
    new ObjectOutputStream(out).writeObject(82L)
  }

  def kryo_device_setup(_dev: String) {
    val dev = new RandomAccessFile(_dev, "rw")
    val out = new DeviceOutputStream(dev, 0L)
    val output = new Output(out)
    kryo.writeObject(output, 82L);
    output.close()
  }

  def main (args: Array[String]) {
    for ( dev <- config.getStringList("storage.devices") ) {
      config.getString("storage.engine") match {
        case "kryo" => kryo_device_setup(dev)
        case "device" => device_setup(dev)
        case "driver" => driver_setup(dev)
      }
    }
  }
}
