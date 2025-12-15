import KernelKeys._

ThisBuild / kernelVersion := sys.env.getOrElse("KERNEL_VERSION", "4.1.0-SNAPSHOT")
