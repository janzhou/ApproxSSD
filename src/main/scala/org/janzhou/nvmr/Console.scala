package org.janzhou.nvmr

object console {
  val print_level = config.getInt("console.print_level")

  val print_error = 0
  val print_warn  = 1
  val print_info  = 2
  val print_debug = 3

  private val prefix = Array("[error] ", "[warn] ", "[info] ", "[debug] ")

  private def console_print(s:Any, level:Int) {
    if ( print_level >= level ) {
      println(prefix(level) + s)
    }
  }

  def debug(s:Any) = console_print(s, print_debug)
  def info(s:Any) = console_print(s, print_info)
  def log(s:Any) = info(s)
  def warn(s:Any) = console_print(s, print_warn)
  def error(s:Any) = console_print(s, print_error)
}
