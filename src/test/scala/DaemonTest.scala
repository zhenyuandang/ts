package scala

import cn.local.deamon.Daemon
import org.junit.Test

class DaemonTest{

  // paramNum is 5
  @Test
  def test1() {
    Daemon.countDate(5)
  }

  // ParamNum is 7
  @Test
  def test2() {
    Daemon.countDate(7)
  }

}
