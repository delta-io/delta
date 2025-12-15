import sbt._

object KernelKeys {
  val kernelVersion = settingKey[String]("Kernel artifact version")
}
