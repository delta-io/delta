# ✅ URI 编码问题修复 - 最终完成

## 🎯 测试结果
**所有 11 个 idempotent write 测试 100% 通过！**

```
✅ idempotent write: idempotent DataFrame insert
✅ idempotent write: idempotent SQL insert  
✅ idempotent write: idempotent DeltaTable merge
✅ idempotent write: idempotent SQL merge
✅ idempotent write: idempotent DeltaTable update
✅ idempotent write: idempotent SQL update
✅ idempotent write: idempotent DeltaTable delete
✅ idempotent write: idempotent SQL delete
✅ idempotent write: valid txnVersion
✅ idempotent write: auto reset txnVersion
✅ idempotent writes in streaming foreachBatch

Run completed in 37 seconds
Total: 11/11 通过 (100%) ✅
```

## 📝 完整修改

### 修改 1：SparkTable.java - 解码表路径

**文件**：`kernel-spark/src/main/java/io/delta/kernel/spark/catalog/SparkTable.java`

```java
public SparkTable(
    Identifier identifier, org.apache.spark.sql.catalyst.catalog.CatalogTable catalogTable) {
  this(
      identifier,
      getDecodedPath(requireNonNull(catalogTable, "catalogTable is null").location()),  // ← 修改
      Collections.emptyMap(),
      Optional.of(catalogTable));
}

/**
 * Helper method to decode URI path handling URL-encoded characters correctly.
 * E.g., converts "spark%25dir%25prefix" to "spark%dir%prefix"
 */
private static String getDecodedPath(java.net.URI location) {
  try {
    // Use new File(URI).getPath() to get properly decoded filesystem path
    return new java.io.File(location).getPath();
  } catch (IllegalArgumentException e) {
    // Fallback to toString() if URI is not a file:// URI
    return location.toString();
  }
}
```

**效果**：Delta Kernel 可以正确找到 `_delta_log` 目录

### 修改 2：SparkScan.java - 解码数据文件路径

**文件**：`kernel-spark/src/main/java/io/delta/kernel/spark/read/SparkScan.java`

```java
// Build file path: decode the relative path from AddFile first (it may be URL-encoded),
// then combine with table path, and finally convert to URI
final String decodedRelativePath =
    java.net.URLDecoder.decode(addFile.getPath(), java.nio.charset.StandardCharsets.UTF_8);  // ← 新增
final String filePath = java.nio.file.Paths.get(tablePath, decodedRelativePath).toString();  // ← 修改
final java.net.URI fileUri = new java.io.File(filePath).toURI();  // ← 新增

final PartitionedFile partitionedFile =
    new PartitionedFile(
        getPartitionRow(addFile.getPartitionValues()),
        SparkPath.fromPath(new org.apache.hadoop.fs.Path(fileUri)),  // ← 修改
        0L,
        addFile.getSize(),
        locations,
        addFile.getModificationTime(),
        addFile.getSize(),
        otherConstantMetadataColumnValues);
```

**效果**：Spark 可以正确读取包含特殊字符的数据文件

## 🔑 关键修复点

### 1. 解码 CatalogTable 路径
- **修改前**：`catalogTable.location().toString()` → 保留 URL 编码 `spark%25dir%25prefix`
- **修改后**：`getDecodedPath(catalogTable.location())` → 解码为 `spark%dir%prefix`

### 2. 解码 AddFile 路径  
- **修改前**：`addFile.getPath()` → 保留 URL 编码 `test%25file%25prefix`
- **修改后**：`URLDecoder.decode(addFile.getPath())` → 解码为 `test%file%prefix`

### 3. 正确的路径拼接
- **修改前**：字符串拼接 `tablePath + addFile.getPath()`
- **修改后**：`Paths.get(tablePath, decodedRelativePath)`

### 4. 正确的 URI 转换
- **修改前**：`SparkPath.fromUrlString()` → 会尝试解析 URL 字符串
- **修改后**：`SparkPath.fromPath(new Path(uri))` → 直接使用 URI

## 📊 URL 编码处理流程

### 完整的编码/解码链

```
1. Hive Metastore/Catalog
   ↓
   URI: file:///tmp/spark%25dir%25prefix  (URL 编码存储)
   ↓
2. catalogTable.location()
   ↓
   URI object
   ↓
3. new File(URI).getPath()  ← 解码步骤 1
   ↓
   String: "/tmp/spark%dir%prefix"  (文件系统路径)
   ↓
4. Delta Kernel 访问
   ↓
   ✅ 成功找到 _delta_log

5. AddFile 从 Delta Log 读取
   ↓
   String: "test%25file%25prefix-part-00000.parquet"  (URL 编码)
   ↓
6. URLDecoder.decode()  ← 解码步骤 2
   ↓
   String: "test%file%prefix-part-00000.parquet"  (解码)
   ↓
7. Paths.get(tablePath, decodedPath)
   ↓
   String: "/tmp/spark%dir%prefix/test%file%prefix-part-00000.parquet"
   ↓
8. new File(path).toURI()  ← 重新编码
   ↓
   URI: file:///tmp/spark%25dir%25prefix/test%25file%25prefix-part-00000.parquet
   ↓
9. SparkPath.fromPath(new Path(uri))
   ↓
   ✅ 成功读取文件
```

## 🎓 核心原则总结

| 场景 | 使用 API | 原因 |
|------|---------|------|
| **URI → 文件系统路径** | `new File(URI).getPath()` | 自动解码 URL 编码 |
| **字符串 → 文件系统路径** | `URLDecoder.decode(str)` | 手动解码 URL 编码 |
| **文件系统路径 → URI** | `new File(path).toURI()` | 自动编码特殊字符 |
| **路径拼接** | `Paths.get(parent, child)` | 跨平台安全拼接 |
| **Spark Path** | `SparkPath.fromPath(hadoopPath)` | 避免二次 URI 解析 |

## ⚠️ 不要这样做

```java
// ❌ 错误：直接 toString() 保留了 URL 编码
catalogTable.location().toString()

// ❌ 错误：字符串拼接无法处理特殊字符
tablePath + "/" + addFile.getPath()

// ❌ 错误：fromUrlString 会尝试解析导致错误
SparkPath.fromUrlString(path)

// ❌ 错误：混合编码和未编码路径
Paths.get(decodedPath, encodedPath)
```

## ✅ 应该这样做

```java
// ✅ 正确：解码 URI 到文件系统路径
new File(catalogTable.location()).getPath()

// ✅ 正确：解码 URL 编码的字符串
URLDecoder.decode(addFile.getPath(), StandardCharsets.UTF_8)

// ✅ 正确：安全拼接文件系统路径
Paths.get(tablePath, relativePath).toString()

// ✅ 正确：文件系统路径转 URI（自动编码）
new File(filePath).toURI()

// ✅ 正确：使用 fromPath 传递 URI
SparkPath.fromPath(new Path(fileUri))
```

## 📁 修改的文件

1. **kernel-spark/src/main/java/io/delta/kernel/spark/catalog/SparkTable.java**
   - ➕ 添加 `getDecodedPath(URI)` 静态方法（14行）
   - ✏️ 修改 `CatalogTable` 构造函数调用 `getDecodedPath()`（1行）

2. **kernel-spark/src/main/java/io/delta/kernel/spark/read/SparkScan.java**
   - ✏️ 修改文件路径拼接逻辑（6行）
   - ➕ 添加 `URLDecoder.decode()` 调用
   - ✏️ 修改使用 `Paths.get()` 拼接
   - ✏️ 修改使用 `File.toURI()` 编码
   - ✏️ 修改使用 `SparkPath.fromPath()`

**总计**：2 个文件，约 20 行代码修改

## 🚀 运行测试

```bash
# 单个测试
./build/sbt "testOnly org.apache.spark.sql.delta.DeltaSuite -- -z \"idempotent SQL update\""

# 所有 idempotent write 测试
./build/sbt "testOnly org.apache.spark.sql.delta.DeltaSuite -- -z \"idempotent write\""

# 完整 DeltaSuite
./build/sbt "testOnly org.apache.spark.sql.delta.DeltaSuite"
```

## 🎉 结论

通过正确处理 URL 编码和文件系统路径的转换，成功修复了所有 idempotent write 测试。

**关键要点**：
- 始终明确当前处理的是 URL 编码还是文件系统路径
- 使用正确的 Java API 进行编码/解码
- 在路径拼接前统一解码
- 在需要 URI 时重新编码

---

**修复完成**：2025-10-24  
**测试通过率**：11/11 (100%) ✅  
**修改文件数**：2 个  
**代码行数**：~20 行

