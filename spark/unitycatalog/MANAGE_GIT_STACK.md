# Unity Catalog Git Stack Management

## Overview
This document describes how to manage the git stack for Unity Catalog integration PRs to the Delta GitHub repository.

## Branch Naming Convention

**All PR branches use the working branch name as prefix: `spark-uc-`**

This follows the global branch naming convention: `~/.config/dev-conventions/BRANCH_NAMING_CONVENTION.md`

## Current Branch Structure

```
origin/master
    ↓
spark-uc-pr1-foundation      ← PR 1: Foundation + Repository Setup (~620 lines)
    ↓
spark-uc-pr2-dml-framework   ← PR 2: SQLExecutor + DML (~549 lines)
    ↓
spark-uc-pr3-ddl             ← PR 3: DDL Operations (~274 lines)
    ↓
spark-uc-pr4-utility         ← PR 4: Utility Operations (~251 lines)
    ↓
spark-uc-pr5-read            ← PR 5: Advanced Read (~260 lines)
```

## Push All Branches to Fork

```bash
cd /Users/tdas/Projects/delta

# Push all PR branches to your fork
git push tdas spark-uc-pr1-foundation
git push tdas spark-uc-pr2-dml-framework  
git push tdas spark-uc-pr3-ddl
git push tdas spark-uc-pr4-utility
git push tdas spark-uc-pr5-read
```

## Create PRs on GitHub

Create PRs in this order, each targeting the Delta `master` branch:

### PR 1: Unity Catalog Foundation
- **Branch**: `spark-uc-pr1-foundation`
- **Target**: `delta-io/delta:master`
- **Title**: Unity Catalog Foundation: Core server integration and test framework
- **Description**: Establishes the foundational Unity Catalog integration for Delta Lake, including UC server lifecycle management and basic connectivity validation.

### PR 2: SQLExecutor Framework + DML Operations  
- **Branch**: `spark-uc-pr2-dml-framework`
- **Target**: `delta-io/delta:master` 
- **Title**: Unity Catalog SQLExecutor framework and comprehensive DML operations
- **Description**: Adds pluggable SQL execution framework and comprehensive DML test suite (INSERT, UPDATE, DELETE, MERGE) for Unity Catalog managed tables.

### PR 3: DDL Operations
- **Branch**: `spark-uc-pr3-ddl`
- **Target**: `delta-io/delta:master`
- **Title**: Unity Catalog DDL operations test suite
- **Description**: Comprehensive validation of Data Definition Language operations including table creation, schema management, and metadata operations.

### PR 4: Utility Operations
- **Branch**: `spark-uc-pr4-utility`  
- **Target**: `delta-io/delta:master`
- **Title**: Unity Catalog utility operations test suite
- **Description**: Validation of Delta utility operations including OPTIMIZE, ZORDER, DESCRIBE HISTORY, and catalog metadata queries.

### PR 5: Advanced Read Operations
- **Branch**: `spark-uc-pr5-read`
- **Target**: `delta-io/delta:master`
- **Title**: Unity Catalog advanced read operations and time travel
- **Description**: Unity Catalog-specific read capabilities including time travel, access patterns, and schema evolution support.

## Managing the Stack

### After PR Merge
When a PR is merged, you may need to rebase the subsequent PRs:

```bash
# After PR1 is merged, rebase PR2 onto the new master
git checkout pr2-uc-dml-framework
git rebase origin/master

# Push the rebased branch (force push required)
git push tdas pr2-uc-dml-framework --force-with-lease

# Repeat for subsequent PRs
```

### Testing Individual PRs
Each branch can be tested independently:

```bash
# Test PR1 
git checkout pr1-uc-foundation
build/sbt -DsparkVersion=master "sparkUnityCatalog/test"

# Test PR2 (requires PR1 changes)
git checkout pr2-uc-dml-framework  
build/sbt -DsparkVersion=master "sparkUnityCatalog/test"

# And so on...
```

### Updating PRs
To update a PR with new changes:

```bash
# Make changes on the appropriate branch
git checkout pr2-uc-dml-framework
# ... make changes ...
git add -A && git commit -m "Address review feedback"

# Push updated branch
git push tdas pr2-uc-dml-framework

# If this affects subsequent PRs, rebase them
git checkout spark-uc-pr3-ddl
git rebase pr2-uc-dml-framework
git push tdas spark-uc-pr3-ddl --force-with-lease
```

## Stack Benefits

1. **Independent Review**: Each PR can be reviewed separately for its specific functionality
2. **Incremental Merge**: PRs can be merged as they're approved, allowing parallel review  
3. **Risk Management**: Issues in later PRs don't block earlier foundational work
4. **Clear Scope**: Each PR has a well-defined, testable scope
5. **Rollback Safety**: Individual PRs can be reverted without affecting others

## Current Status

- ✅ All 6 PR branches created and tested locally
- ✅ Each branch contains only the relevant changes for its scope
- ✅ All tests pass on each branch independently  
- ⏳ Ready to push to fork and create GitHub PRs

## Notes

- All branches are clean and contain no build artifacts or secrets
- Each PR is self-contained and thoroughly tested
- The stack follows logical dependency order
- Total test coverage: 43 tests across all Unity Catalog functionality
