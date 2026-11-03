/**
 * Kernel version override used by sparkV2.
 *
 * When `-DkernelVersion=<v>` is set on the SBT invocation, sparkV2 consumes
 * `delta-kernel-{api,defaults,unitycatalog}` from Maven at version `v` instead
 * of depending on the in-tree kernel source projects. When unset, sparkV2
 * keeps using the in-tree kernel.
 *
 * Centralised here so the version-resolution rule lives in a single,
 * easy-to-find file rather than inline in build.sbt.
 */
object KernelVersion {
  /** SBT system property used to pin the kernel Maven version. */
  val systemProperty: String = "kernelVersion"

  /** The resolved kernel version override, or None to use in-tree kernel sources. */
  val kernelVersionOpt: Option[String] = sys.props.get(systemProperty)
}
