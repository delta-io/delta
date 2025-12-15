import KernelKeys._

ThisBuild / kernelVersion := sys.env.getOrElse("KERNEL_VERSION", "1.0.0-SNAPSHOT")
