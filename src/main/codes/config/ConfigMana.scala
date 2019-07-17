package config

import java.io.InputStream
import java.util.Properties

object ConfigMana {

  val prop = new Properties()

  try{
    val params: InputStream = ConfigMana.getClass.getClassLoader.getResourceAsStream("params.properties")
    prop.load(params)
  }

}
