import KernelKeys._

ThisBuild / kernelVersion := sys.env.getOrElse("KERNEL_VERSION", "0.1.0-SNAPSHOT")
