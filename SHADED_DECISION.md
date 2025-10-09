# delta-spark-shaded 决策分析

## TL;DR

**推荐：方案 C - 保留空模块**
- 成本低（只是一个空目录）
- 保持架构灵活性
- 未来如需 delegation 可随时添加

---

## 当前状态

delta-spark-shaded 当前为空：
- 源码：`spark-shaded/src/main/scala/` 空目录
- Jar：288 字节（只有 MANIFEST）
- 依赖：v1 + v2

## 是否必要？

### ❌ 不必要的情况

1. **V1 和 V2 类名不冲突**
   - V1: `org.apache.spark.sql.delta.catalog.DeltaCatalog`
   - V2: `io.delta.kernel.spark.table.SparkTable`
   - 不同的包名和类名，可以共存

2. **用户可以直接选择实现**
   ```scala
   // 方式1: 用 V1 Catalog
   spark.conf.set("spark.sql.catalog.spark_catalog", 
                  "org.apache.spark.sql.delta.catalog.DeltaCatalog")
   
   // 方式2: 用 V2 Table
   spark.read.format("io.delta.kernel.spark").load(path)
   ```

3. **不需要同时启用**
   - 如果 V1 和 V2 是互斥的，不需要 delegation

### ✅ 必要的情况

1. **需要统一入口点**
   ```scala
   // 统一的 DeltaCatalog，内部路由到 V1 或 V2
   class DeltaCatalog extends ... {
     def loadTable(...) = {
       if (useKernel) v2.SparkTable(...)
       else v1.DeltaTable(...)
     }
   }
   ```

2. **需要同时注册 V1 和 V2**
   ```scala
   class DeltaSparkSessionExtension {
     override def apply(extensions: SparkSessionExtensions) = {
       registerV1Rules(extensions)
       registerV2Rules(extensions)
     }
   }
   ```

3. **需要平滑迁移**
   - 逐步从 V1 迁移到 V2
   - A/B 测试不同实现
   - 按功能分流（读用 V2，写用 V1）

4. **需要 Shading（名字冲突）**
   - 如果 V1 和 V2 有同名类
   - 使用 sbt-assembly shading 规则

## 三种方案对比

| 方案 | 优点 | 缺点 | 适用场景 |
|------|------|------|----------|
| **A. 保留并实现** | • 统一入口<br>• 灵活切换<br>• 平滑迁移 | • 额外代码<br>• 维护成本 | 需要 delegation |
| **B. 完全删除** | • 代码最简<br>• 依赖清晰 | • 未来加回成本高<br>• 缺少灵活性 | 确定不需要 delegation |
| **C. 保留空模块** | • 架构预留<br>• 无额外成本<br>• 随时可加 | • 多一个模块 | **推荐：暂不确定** |

## 推荐方案：C（保留空模块）

### 理由

1. **成本极低**
   - 只是一个空目录 + 288B jar
   - 不影响编译和发布

2. **架构清晰**
   ```
   v1 (prod) ──┐
               ├──> shaded (delegation) ──> spark (final jar)
   v2 (prod) ──┘
   ```

3. **未来灵活**
   - 如果需要 delegation，直接添加代码
   - 不需要重构 build.sbt

### 何时添加代码到 shaded？

**触发条件**：
- [ ] 需要根据配置自动选择 V1/V2
- [ ] 需要同时启用 V1 和 V2
- [ ] 发现类名冲突
- [ ] 需要 A/B 测试或灰度发布

**暂时不需要**：
- 用户可以显式选择 V1 或 V2
- 两个实现可以独立使用

## 如何删除 delta-spark-shaded（如果确定不需要）

```scala
// build.sbt 修改

// 删除 delta-spark-shaded 模块定义
// lazy val `delta-spark-shaded` = ...

// spark 模块直接依赖 v1 和 v2
lazy val spark = (project in file("spark-tests"))
  .dependsOn(`delta-spark-v1`)
  .dependsOn(`delta-spark-v2`)
  .settings(
    Compile / packageBin / mappings := {
      val v1 = (`delta-spark-v1` / Compile / packageBin / mappings).value
      val v2 = (`delta-spark-v2` / Compile / packageBin / mappings).value
      v1 ++ v2  // 移除 shaded
    }
  )
```

```bash
# 删除目录
rm -rf spark-shaded/
```

## 决策

✅ **暂时保留空的 delta-spark-shaded**

原因：
- 成本可忽略
- 保持架构扩展性
- 符合原始设计意图
- 未来如需 delegation 可随时添加


