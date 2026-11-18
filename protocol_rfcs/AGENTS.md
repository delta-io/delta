# AGENTS.md — Writing and reviewing Delta protocol changes and RFCs

These are instructions for an AI agent writing or reviewing changes to the
Delta transaction log protocol. That means any change touching `PROTOCOL.md` or
anything under `protocol_rfcs/` (new RFCs, RFC updates, acceptances and
rejections).

The file has five parts:

| Part | What it covers |
|---|---|
| [1. Process](#1-process) | PR types, and one checklist per type: what to do and what gets checked. |
| [2. Protocol design rules](#2-protocol-design-rules) | What makes a protocol change safe: features, reader and writer rules, compatibility, interactions. |
| [3. Writing the spec text](#3-writing-the-spec-text) | Drafting an RFC, normative language, examples, engine neutrality, `PROTOCOL.md` style. |
| [4. Reviewing a protocol PR](#4-reviewing-a-protocol-pr) | The review procedure, common pitfalls, and the required output format. |
| [5. Self-review before opening the PR](#5-self-review-before-opening-the-pr) | For authors: run the review on your own draft. |

- **Writing** a protocol change: follow Parts 1–3, then Part 5.
- **Reviewing** a protocol PR: follow Part 4, which uses Parts 1–3 as the
  checklist.

The repo-wide [`AGENTS.md`](../AGENTS.md) at the root points here from its
"Protocol and RFC changes" section, so PRs that change only the root
`PROTOCOL.md` are covered too. Keep all protocol-specific detail in this file;
the root section is only a pointer and a short summary.

The rules come from the review history of these PRs. That covers about
145 PRs touching `PROTOCOL.md` (2019–2026) and 44 PRs touching `protocol_rfcs/`
(about 900 review comments). PR numbers in brackets, e.g. [#4094], point to
historical PRs in `delta-io/delta` where a reviewer raised that concern.

The protocol is a standard that many independent engines implement
(delta-spark, Delta Kernel Java/Rust, delta-rs, Trino, and others). A vague or
wrong sentence can lead to wrong query results or data corruption in an engine
the author never tested. Write and review as if every reader and writer will
follow the text exactly as written, and nothing more.

---

## 1. Process

`protocol_rfcs/README.md` defines the RFC process. This part turns it into one
checklist per PR type. Authors follow the checklist in order; reviewers check
each item.

### 1.1 PR types

Every protocol PR is exactly one of these types.

| Type | How to recognize it |
|---|---|
| **NEW-RFC** | Adds a new file `protocol_rfcs/<name>.md` and a "Proposed RFCs" row in `protocol_rfcs/README.md`. |
| **RFC-UPDATE** | Changes an existing proposed RFC in `protocol_rfcs/*.md`. |
| **RFC-ACCEPT** | Moves an RFC to `protocol_rfcs/accepted/`, merges its text into `PROTOCOL.md`, and moves the README row. |
| **RFC-REJECT** | Moves an RFC to `protocol_rfcs/rejected/`. |
| **DIRECT-SPEC** | Changes `PROTOCOL.md` meaning without an RFC (a clarification, a bug fix, or making the spec match long-standing implementation behavior). |
| **EDITORIAL** | Only typos, grammar, broken links, TOC or formatting changes in `PROTOCOL.md` or an RFC. No change in meaning. |

Two problems to watch for in any PR:
- **Wrong type.** For example, a DIRECT-SPEC PR that adds a new table feature,
  a new action field, or a new requirement that old clients could break. Those
  need an RFC [#5494 → reverted in #5708, #6798]. Another example: an EDITORIAL
  PR that quietly changes meaning (for example "should" → "must", or deleting a
  sentence).
- **Mixed PR.** Spec changes bundled with engine code or unrelated cleanups.
  The spec part goes in its own PR [#3378 → #3398, #1742].

### 1.2 All PR types
- [ ] PR title starts with `[PROTOCOL]` (or similar) and describes the change
      accurately. Keep the title and description current after revisions
      [#3750, #6175, #7148].
- [ ] The PR description explains **why** the change is needed and what
      problem it solves. For clarifications, it explains what the current text
      gets wrong or leaves undefined [#4179, #2712].
- [ ] The PR doesn't reformat existing Markdown tables or rewrap lines it
      doesn't otherwise change [#2808, #1588].

### 1.3 NEW-RFC: proposing a new RFC
- [ ] A GitHub issue of type Protocol Change Request exists, and there is
      basic consensus there that the feature should exist. All discussion
      happens on the issue.
- [ ] The RFC file is a copy of `protocol_rfcs/template.md` saved as
      `protocol_rfcs/<feature-name>.md`, with a meaningful kebab-case name.
      Every template section is filled in or marked "Not applicable because
      …" (Part 3.1). That includes the feature summary, the compatibility and
      interaction tables, and the spec text below the separator.
- [ ] The issue link at the top of the RFC has the right number:
      `**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/N**`
      [#7272].
- [ ] Exactly one new row in the **Proposed RFCs** table in
      `protocol_rfcs/README.md`, with date proposed, a working link to the RFC
      file, the issue link and the title [#2599]. (The README text mentions
      `index.md`, but the real index is `protocol_rfcs/README.md`.)
- [ ] The table feature and any new table properties use a temporary `-dev`
      suffix while the RFC is proposed. `protocol_rfcs/README.md` strongly
      recommends this but doesn't require it. The suffix tells users the
      feature is experimental, with no compatibility guarantee. If the RFC
      doesn't use one, it says why. (Reviewers: NIT unless there is a reason
      the suffix matters for this feature.)
- [ ] If this RFC replaces an earlier one, it says why the earlier one is
      superseded, and the PR moves the earlier one to `rejected/` [#4382].
- [ ] The author has run the self-review (Part 5).
- [ ] The PR title has a `[PROTOCOL]` tag. The PR links the issue with "see #N"
      or "issue #N", **never** `closes` / `fixes` / `resolves`, because merging
      a proposed RFC must not close the issue.
- [ ] The PR changes only the RFC file and the README row. Engine code for the
      feature must not merge into master until the RFC PR merges, and until
      acceptance it stays behind feature flags so existing users aren't
      affected.

### 1.4 RFC-UPDATE: updating a proposed RFC
This covers an RFC whose PR has merged, so the file is on master at
`protocol_rfcs/<feature-name>.md` and listed under **Proposed RFCs**, but it
has not been accepted. (Before the RFC PR merges, push changes to that PR.)
Each update is a new PR that edits the RFC file.
- [ ] The branch is rebased on current master. RFC text written against an
      outdated master can undo newer changes [#6696].
- [ ] Every use of the changed term or rule is updated together: search the
      RFC, `PROTOCOL.md`, other RFCs and all examples [#6696].
- [ ] If the RFC proposes changes to existing `PROTOCOL.md` text, they are
      described in the RFC below the separator. `PROTOCOL.md` itself is not
      edited before the acceptance PR.
- [ ] The PR description says whether the change is **editorial** or
      **changes meaning**, and what implementation experience or ambiguity
      prompted it.
- [ ] Making a rule looser doesn't weaken an unrelated safety rule. Making a
      rule stricter comes with a story for existing preview tables and older
      writers [#5166, #4537].
- [ ] If the design has changed so much that it's a different proposal, this
      should be a new RFC that explains why the old one is superseded, with
      the old one moved to `rejected/` [#4382].

### 1.5 Changing a feature after its RFC is accepted
Once an RFC is accepted, its spec lives in `PROTOCOL.md`, and the file in
`protocol_rfcs/accepted/` is a historical record. Don't edit that file to
change the feature. Instead:
- If the change is fully backward compatible (a clarification, a fix to a clear
  mistake, or writing down behavior all major implementations already have),
  edit `PROTOCOL.md` directly as a DIRECT-SPEC PR (1.8).
- Otherwise (a new requirement, field, action or table feature), propose a new
  RFC that names the accepted RFC it changes.

### 1.6 RFC-ACCEPT: accepting an RFC
- [ ] **The acceptance criteria in `protocol_rfcs/README.md` are met and backed
      by evidence:** a thoroughly tested production implementation (for
      example in delta-spark), and at least a discussion, ideally a prototype,
      showing that Delta Kernel can implement it. If evidence is missing, ask
      "is there a production implementation?" [#7326]. That a lot of time has
      passed is not evidence.
- [ ] External specs the RFC depends on are stable (for example a Parquet spec
      change) [#4096].
- [ ] **Merge fidelity.** The RFC's spec text (everything below the `--------`
      separator) is merged into `PROTOCOL.md`, keeping its meaning sentence by
      sentence. No meaning change, dropped requirement, or deleted existing
      text, such as a cleanup step that is no longer mentioned [#6066]. If
      implementation experience changed the design, the RFC file is updated in
      the same PR so the two match.
- [ ] The spec matches what the production implementation actually writes:
      names, serialized forms, edge cases, and feature removal.
- [ ] If the RFC used a `-dev` suffix, it is dropped from feature and property
      names in the spec. The code drops it too, in its own engine PR [#3416].
- [ ] The feature is added to **"Valid Feature Names in Table Features"** in
      the `PROTOCOL.md` appendix, each new property to the **Table
      Properties** table, and TOC entries for the new sections
      [#2808, #2868, #7650, #6696].
- [ ] The interaction table in Part 2.5 of this file is updated in the same
      PR, adding a row for the newly accepted feature if other features must
      account for it, so the table always reflects the features actually in
      `PROTOCOL.md`.
- [ ] The RFC file is moved with `git mv` to `protocol_rfcs/accepted/`, and
      links to the old path are fixed [#6066].
- [ ] The README row is moved from Proposed to **Accepted**, with "Date
      accepted".
- [ ] The PR description links the production implementation (its PRs and its
      tests) and the Kernel feasibility discussion or prototype, and
      summarizes any differences between the final spec and the proposed RFC.
- [ ] The PR uses "closes #N", and the issue title gets an `[ACCEPTED]`
      marker.
- [ ] The whole of Part 2 is re-checked on the final text. Acceptance locks in
      compatibility promises: cross-feature requirements, feature removal and
      preview migration must be resolved now [#4094].

### 1.7 RFC-REJECT: rejecting an RFC
- [ ] The RFC file is moved with `git mv` to `protocol_rfcs/rejected/`.
- [ ] The README row is moved to **Rejected**, with the date.
- [ ] The PR uses "closes #N", and the issue title gets a `[REJECTED]` marker.
- [ ] There is a plan to remove any experimental code, in its own engine PR.

### 1.8 DIRECT-SPEC: changing `PROTOCOL.md` without an RFC
Direct edits are the exception. Maintainers have said a direct edit "should
absolutely not be used as a precedent for willy nilly changing the protocol"
[#6699, #7496]. One is allowed only if **all** of these hold, and the PR says
which one justifies it:
- [ ] It is fully backward compatible: no new requirement that existing
      conforming readers or writers would break [#6324].
- [ ] It does one of these:
  - writes down behavior every major implementation already has (cite the
    evidence) [#5355, #4923, #6966]
  - fixes a clear mistake in the spec or an example [#7531, #5640]
  - removes ambiguity without picking a winner between existing
    implementations
- [ ] The author has checked major implementations (delta-spark, Kernel
      Java/Rust, delta-rs, and ideally Trino) and they conform, or the PR
      explains what happens to those that don't [#7496, #5495].
- Otherwise it needs an RFC, or a `-dev` experimental field written only by
  one implementation until the design settles [#6798].

### 1.9 EDITORIAL
- [ ] Nothing changes meaning. Check every edited sentence, especially
      normative keywords and deleted text.
- [ ] TOC anchors still resolve after any heading change.

---

## 2. Protocol design rules

Authors use this part to design the change and fill in the RFC's feature
summary, compatibility table and interaction table. Reviewers go through every
item. For each one, either raise a finding or be ready to say why it doesn't
apply. "Not affected" needs a reason; don't assume it.

### 2.1 Is a table feature needed, and for whom?

Reviewers argue about this choice more than any other. Answer these questions
in order and write the reasoning in the RFC's feature summary.

1. **Does any client need to understand the feature at all?** Any new action,
   field, file, or data that readers or writers must understand
   ("load-bearing") needs a table feature covering it [#4096, #1742]. If
   ignoring it is always safe for both readers and writers, a table feature
   may not be needed. Log compaction files, for example, are optional to read
   and to write [#2122]. Explain why in the RFC.
2. **Would an old reader that ignores the feature return exactly the same
   results as a reader that understands it, for every table state the feature
   allows?** "Ignores" means the old reader follows the older protocol without
   recognizing any new action, field, property, file meaning or data encoding.
   Work out what it would actually produce. Don't assume it safely skips whole
   files unless the existing protocol requires that. "Results" covers
   everything a reader does:
   - scan results
   - filters, and data skipping with statistics
   - partition pruning
   - time travel
   - change data feed reads
   - reading checkpoints

   If any of these can differ, the feature must apply to **readers and
   writers**.
3. **Otherwise, writers only** is possible, but only if correctness depends
   just on writers following the new rules, for example keeping an invariant
   or writing extra metadata. The writer feature is what stops old writers
   from breaking those rules.

   This includes features that store optional information in the log that is
   not needed for correctness but matters for read performance (for example
   extra statistics or other data-skipping metadata). Old readers can ignore
   it and still return correct results, just more slowly. But writers must
   know about it explicitly, so that they at least preserve it when they
   write checkpoints instead of silently dropping it. Such a feature is a
   candidate for a writers-only feature. Check performance as well as
   correctness: say what happens to read performance when an old writer or
   an old checkpoint writer touches the table.
4. Write down why ignoring the feature still gives correct results. "Old
   readers ignore it" is not enough on its own [#2808, #2790].

Examples from `PROTOCOL.md`:
- **Readers and writers:**
  - `deletionVectors`: an old reader that ignores deletion vectors returns
    deleted rows.
  - `columnMapping`: an old reader looks up columns by their logical names,
    but the data files use physical names.
- **Writers only:**
  - `appendOnly`, `checkConstraints` and `generatedColumns`: readers see the
    same data, and only writers have to enforce the rules.
  - `rowTracking`.
- **A warning example:** collations [#3068]. Without a reader feature, an old
  reader compares strings with the default ordering and returns wrong query
  results.

### 2.2 Table feature definition
- [ ] Exact feature name: lowerCamelCase, plural where natural
      (`deletionVectors`, `checkConstraints`), consistent everywhere [#1450].
      A shipped feature name can't be renamed [#1747].
- [ ] Minimum protocol versions (reader 3 / writer 7 for table features), and
      required features, listed explicitly. Examples: `variantShredding`
      requires `variantType` [#6696]; row tracking requires `domainMetadata`
      [#3740]. Also list features that can't be used together (for example
      clustering and partitioning) [#2294].
- [ ] **Supported vs enabled.** In the spec, a feature in the protocol is
      *supported*. A table property (for example `delta.enableX`) makes it
      *enabled*/active. The text must say what readers and writers do in each
      state. Usually readers must handle the feature's data whenever it is
      supported, even if the property is false or missing [#6696, #1780,
      #5619]. If the feature has no property and being supported already makes
      it active, say so [#7650]. Don't say a table "contains" a feature
      [#6696].
- [ ] Enablement rules: when a writer may add the feature (the first commit
      that uses it may also enable it), and what turning the property off or
      removing it means [#1450].
- [ ] **Removing the feature.** What writers must do before the feature can be
      dropped from the protocol, and what readers can assume afterwards
      [#4094, #5016].

### 2.3 Reader and writer requirements
- [ ] Separate `Reader Requirements for <Feature>` and
      `Writer Requirements for <Feature>` subsections (say "Writer", not
      "Write"), each with complete instructions [#1742, #2808, #2122].
- [ ] Writers are also readers. A writer must meet all reader requirements
      [#1450].
- [ ] Every obligation is covered across the feature's lifecycle: create,
      enable, read, write and append, update and delete (DVs), OPTIMIZE and
      maintenance, checkpoint, log compaction, metadata cleanup and VACUUM,
      CLONE and RESTORE, upgrading existing tables, and feature removal.
- [ ] What readers must do when they find state they don't understand or that
      is invalid: an unknown domain, an unknown codec value, a malformed field.
      Say whether they fail, ignore it, or keep it unchanged [#1742, #6324].
- [ ] If a rule is expensive to check, phrase it as "clients may assume X"
      and say what writers must guarantee. Don't require readers to verify it
      [#4058, #4096].

### 2.4 Compatibility with older clients and existing tables
Write down this matrix and check every cell:
- old reader × new table, old writer × new table
- new reader × old table, new writer × old table
- tables created before the feature was enabled, and a mix of old and new
  files [#7078]
- connectors that support only some of the related features (for example the
  type but not shredding)
- checkpoint writers that don't know the feature. **Will they silently drop
  new columns or fields from checkpoints and sidecars?** [#7413]

For each cell: does the client fail safely (rejected by the protocol check) or
silently give wrong results or corrupt state? Silent wrong results are always
**BLOCKING**.

Also check:
- [ ] Readers ignore unknown fields. New optional fields must be safe to
      ignore, or else covered by a feature [#1450, #4096].
- [ ] Renaming a field or changing its encoding breaks typed serialization in
      kernel-rs and delta-spark [#6798]. Existing written names win over
      "nicer" names [#4923, #3777].
- [ ] Making a field required (or making a "should" a "must") can make
      existing writers non-conformant. Check whether they still conform [#1682].

### 2.5 Interactions with other features
For each feature below, either describe the interaction or say why there is
none:

| Area | What to check |
|---|---|
| Column mapping | Physical vs logical names in new fields, stats, clustering columns, and paths. Nested field ids [#6160, #2264, #6939] |
| Deletion vectors | A "logical file" is `(path, dvId)`. Effect on stats and `numRecords` [#6798, #1450] |
| Checkpoints (classic, multi-part, V2, sidecars) | Checkpoint schema includes new actions and fields. Effect on `_last_checkpoint` [#7413, #6539→#6797, #1946] |
| Log compaction | Whether the new action can be compacted, and the reconciliation rules [#2122, #5190] |
| Metadata cleanup and VACUUM | Which files must be kept, e.g. JSON commits at checkpoint versions for ICT [#5355, #6066] |
| Version checksum (`.crc`) | New fields, and whether they can be derived without a CRC [#3777, #6798] |
| Action reconciliation | Whether new actions take part in log replay. Unique keys and duplicates [#1946, #4058, #4179] |
| Row tracking | Row IDs and row commit versions for new or rewritten files, high-water mark bounds [#7676, #7413] |
| Change data feed | `dataChange`, `_change_type`, CDC files [#3285, #2318] |
| Domain metadata | System vs user domains, tombstones, checkpoint exclusion [#1742, #6337] |
| In-commit timestamps | Whether `commitInfo` must be first. Time-travel effects [#3416] |
| Type widening | Whether new types can widen, and how [#6966, #6082] |
| Generated columns, CHECK constraints, invariants, default columns, identity | Whether those constraints still hold |
| Clustering and partitioning | Can't be used together. Partition values (`partitionValues: {}` is always present). Materialized partitions [#2294, #6545] |
| Statistics and data skipping | Stats for new types, truncation, `tightBounds`, recomputing stats after out-of-line updates [#6082, #7413, #3264] |
| IcebergCompatV1/V2/V3, Iceberg writer compat, AMT | Whether the feature must be blocked under Iceberg compat, field IDs, back references [#4094, #7374, #7413] |
| Catalog-managed tables | Who is responsible for commits and publishing, put-if-absent [#6942] |
| Collations, variant, timestamp_ntz, other types | Ordering and comparison rules |

When a PR changes an existing rule, also update the sections of other features
that depend on it. For example, type widening requires updating the
IcebergCompat sections [#4094], and void type changes affect type widening
[#6966].

### 2.6 Concurrency and conflict detection
- [ ] Which concurrent transactions conflict because of the new actions or
      fields? [#1742]
- [ ] Is there a race to "claim" a resource (files, IDs, domains)? Is it the
      catalog's or the client's job to prevent it? Write that down [#6942,
      #2264].
- [ ] If the motivation is a race, describe it step by step [#2712].

### 2.7 Schemas, fields and allowed values
- [ ] Every new field is listed in a schema table with **Field Name | Data
      Type | Description | optional/required** [#1588, #1890]. Give exact
      types ("String-string map", not "map") [#1946].
- [ ] If a field is optional, say what it means when it's missing and whether
      readers may assume a default [#6982, #3264].
- [ ] Define allowed values completely: enums, ranges, inclusive or exclusive
      bounds, overflow, sign (for example row IDs must be non-negative and the
      initial high-water mark is -1), maximum lengths, case sensitivity, and
      uniqueness rules [#7676, #3961, #6324, #1946].
- [ ] Byte-level semantics where they matter: path comparison (exact bytes,
      before or after URI decoding; relative vs absolute paths), encodings,
      endianness, file naming and padding [#4179, #7148, #2843, #3063].
- [ ] Prefer real fields over `tags` or free-form maps for anything the
      protocol depends on [#2264]. Embedded JSON-in-string needs its schema
      defined [#2264, #1818].
- [ ] Don't add a required field that can only take one value [#1450].
- [ ] Introduce each term or property before using it [#6696].

---

## 3. Writing the spec text

### 3.1 Drafting an RFC
- **Start from [`template.md`](template.md) and fill in every section.** If a
  section doesn't apply, keep the heading and write "Not applicable because
  …". Each template section is something reviewers have repeatedly asked for.
  An empty or missing one usually costs a review round.
- **Write the part below the separator as spec text.** On acceptance it goes
  into `PROTOCOL.md` with little or no rewording: new sections, schema tables,
  reader and writer requirements. A design doc alone is not enough.
- **An RFC comes before the implementation.** Specify what any conforming
  engine must do. Don't describe how a particular implementation works, cite
  its code, or base a requirement on what one codebase happens to do. A
  prototype can be linked from the issue, but the RFC must make sense without
  it. Evidence from implementations belongs in the acceptance PR description
  (1.6).
- **When the RFC changes existing `PROTOCOL.md` text,** quote the current text
  from master, name the section, and show the new text.
- Copy the structure and wording of existing features such as Column Mapping,
  In-Commit Timestamps and Type Widening (3.6).
- Define every new term before using it (4.2, item 9). Say who each
  requirement applies to and when (3.2).
- Make every example complete, valid, and an exact match for the schema
  tables (3.4).
- If you are an agent drafting for an author, don't settle open design
  questions yourself. List them under "Open questions" for the author to
  decide.

### 3.2 Normative language
- [ ] Use RFC 2119 keywords consistently: MUST / MUST NOT / SHOULD / MAY.
      Uppercase is preferred in new text. Flag ambiguous phrasing such as
      "may not", "will", "should avoid" or "is strongly enforced" and ask which
      one is meant [#2266, #2318, #1467, #5190].
- [ ] Each requirement says who it applies to (reader or writer) and when it
      applies (supported vs enabled; which protocol versions).
- [ ] Avoid double negatives and vague quantifiers. Check "all" vs "any" vs
      "some" [#4094, #6798].
- [ ] Be careful with "as of version X" (it means X onward) vs a range [#1450].
- [ ] Make rules as loose as safety allows. Don't add unnecessarily strict
      requirements [#5166].

### 3.3 Ambiguity check
Read each new rule as a hostile but conforming implementer would:
- Which reading lets a writer produce a table that another conforming reader
  misreads?
- Where could two engines both follow the text and still disagree?
- Ask for a concrete "allowed / not allowed" example pair, or a truth table,
  wherever there are several cases [#2266, #6545, #6798].
- Scope questions: table schema or data file schema? Top-level or nested
  fields? Per commit or per snapshot? [#6966, #4058]

### 3.4 Examples
Examples serve as conformance tests. A wrong example is a protocol bug.
- [ ] Every JSON or log example is valid JSON. It matches the schema tables
      and prose exactly, including field names, types, nesting, and fields
      that are always present such as `partitionValues: {}`
      [#7531, #5495, #2294].
- [ ] Encoded examples decode correctly (DV base85, magic numbers,
      endianness) [#2628, #3063].
- [ ] `_delta_log` listings are realistic: no missing versions, correct
      sorting, and counts match the prose [#5640, #1946].
- [ ] Embedded JSON-in-string: show the plain JSON first, then the escaped
      form [#2264].
- [ ] Code samples use one language [#6066].
- [ ] Each new action or concept has at least one complete example, plus
      examples for tricky edge cases [#1450, #2122].

### 3.5 Engine neutrality and spec scope
- [ ] Rules must not depend on Spark, Scala/Java, SQL syntax or a vendor.
      Don't use `Int.MaxValue`; write the number [#3961]. Leave SQL syntax and
      keywords out of the spec unless they are the subject [#2264, #6620,
      #2266]. Don't name vendors [#1742]. Expressions must not depend on one
      engine's SQL dialect [#2240].
- [ ] Leave implementation choices (retention defaults, filename conventions,
      how strictly to validate) to implementations unless they affect
      interoperability [#1682, #6324, #4096, #1494].
- [ ] Prefer established external standards (ISO 8601, the Parquet spec) and
      link to them instead of copying them. Link the external spec once and
      then refer to anchors [#3398, #4096, #6696].
- [ ] Put rationale in the feature introduction. Requirement sections say only
      what to do [#6939, #5893].

### 3.6 Style conventions for `PROTOCOL.md`
Reviewers flag violations as NIT, unless they break links or the TOC.
- Main concepts go in the body. Byte-level formats go in the Appendix, each in
  its own section [#1372, #1494].
- Follow the structure of existing features: intro → `## Enablement` (or
  "Table feature" paragraph) → schemas → `Reader Requirements for X` →
  `Writer Requirements for X` → examples. Copy wording from existing features
  such as Column Mapping, In-Commit Timestamps, Type Widening and
  IcebergCompatV1 [#1372, #6696, #5494].
- Every new section has a **TOC entry** at the top of `PROTOCOL.md` (doctoc
  style), with a correct anchor. Check anchors after renaming headings
  [#2122, #6539, #1742].
- Every new table feature is in **"Valid Feature Names in Table Features"**.
  Every new table property is a row in the **Table Properties** table.
- Use defined spec terms: *data file*, *change data file*, *commit file*,
  *logical file*, *transaction identifier*, *nested field* (vs *column*),
  *supported / enabled*. Don't use "Parquet file" or code class names such as
  `SetTransaction` [#3285, #3787, #6939].
- Use one term per concept throughout. For example, don't mix "domain
  metadata" and "metadata domain" [#1742], or "check constraints" and "CHECK
  constraints" [#1467]. Write feature names out in full in prose and don't
  title-case "table features" [#1450, #2666].
- Use backticks for identifiers, field names, property keys and file name
  patterns (`{unique}.parquet`) [#1946].
- Don't use "here" as link text [#1946]. Cross-link features and sections by
  anchor [#3416, #1450].
- Don't pad Markdown tables with whitespace. Don't reformat existing tables.
- Indent example blocks under the list item they belong to [#6177].
- Use US spelling. Don't hard-code counts that will go stale, like "five types
  of files" [#1300].
- Table property keys use the `delta.` prefix.

---

## 4. Reviewing a protocol PR

### 4.1 Procedure
1. **Classify the PR** as exactly one type from 1.1, and report a wrong type
   or a mixed PR.
2. **Check the process gates:** 1.2 plus the checklist for that type
   (1.3–1.9).
3. **Check protocol safety:** every item in Part 2, for any PR that adds or
   changes meaning. Fill in the compatibility matrix (2.4) and the
   interactions (2.5) yourself, even if the RFC has its own tables.
4. **Check the text:** Part 3.
5. Look for the common causes of long reviews (4.2), and review from each
   perspective in 4.3.
6. **Write the review** in the format in 4.4.

### 4.2 Common causes of many review rounds
Watch for these. They caused most of the long reviews and rejections:
1. Only the happy path is specified. Checkpoints, stats, feature removal,
   existing tables, CLONE/RESTORE and cleanup are left out [#7413, #4094].
2. Reader-writer vs writer-only is treated as a label without proving
   correctness for old readers [#3068].
3. Interactions with other features show up one at a time during review.
   Iceberg compat, AMT, row tracking, CDF and checkpoint schema come up most
   [#4094, #7413, #7374]. Ask for an interaction table up front.
4. Examples, prose and the PR description disagree [#7148, #3750].
5. The text describes one engine's behavior instead of a protocol [#2264,
   #6620].
6. A replacement RFC doesn't explain what was wrong with its predecessor
   [#4382].
7. A spec change goes straight into `PROTOCOL.md` and is later reverted to the
   RFC stage [#5494 → #5708].
8. An RFC is accepted before its implementation or external dependencies are
   stable [#7326, #4096].
9. A new verb or concept ("register", "contains", "well-known", "stripped") is
   used without a definition [#3285, #6696, #6982, #6966].

### 4.3 Review perspectives
Run the review from each of these perspectives. They match the concerns the
most active protocol reviewers raise most often:
- **Adversarial correctness.** Can an old client, or a conforming but
  different client, get wrong results or corrupt the table? Check each rule
  for every legal and illegal state.
- **Physical format and interoperability.** Serialized forms, Parquet and
  Iceberg mapping, field IDs, stats encoding, external spec stability.
- **Process and integration.** Right PR type, README and TOC and appendix
  bookkeeping, consistency with the rest of `PROTOCOL.md`, cross-feature
  sections updated, merge fidelity.
- **Precise wording.** Normative keywords, consistent terms, structure that
  matches existing features, rules no stricter than safety needs.

### 4.4 Output format

Start with a summary:

```
PR type: <NEW-RFC | RFC-UPDATE | RFC-ACCEPT | RFC-REJECT | DIRECT-SPEC | EDITORIAL>
Feature(s): <name, reader-writer | writer-only, min versions, required features>
Verdict: <READY | NEEDS CHANGES | BLOCKED (needs RFC / acceptance criteria not met)>
Process gates: <pass/fail list from Part 1>
Compatibility matrix: <one line per cell from 2.4, marked safe / fails cleanly / UNSAFE>
Interactions considered: <the features from 2.5 that apply, and why each is safe>
```

Then list findings, most severe first. Each finding has:
- **Severity:**
  - `BLOCKING`: correctness or compatibility risk, wrong example, missing
    required bookkeeping, or acceptance criteria not met.
  - `IMPORTANT`: ambiguity, missing interaction, missing lifecycle rule,
    engine-specific wording.
  - `NIT`: style, wording, formatting.
- **Location:** file and line, or section heading.
- **Problem:** quote the text. Describe the concrete scenario that goes wrong
  (which client, which table state, which outcome).
- **Fix:** proposed text as a GitHub ```suggestion``` block whenever possible.

Rules for the reviewer:
- Don't approve or merge. Humans make the final protocol decision. Report
  findings only.
- Don't make up implementation behavior. If a finding depends on what
  delta-spark, Kernel or delta-rs actually do, either check the code (cite the
  file and line) or phrase it as a question to the author.
- Prefer a few precise, well-reasoned findings to a long list of guesses. Keep
  nits grouped and brief.
- Make sure every claim about existing `PROTOCOL.md` text cites the section you
  checked.

---

## 5. Self-review before opening the PR

1. Run the review in Part 4 against your own draft, as if it were someone
   else's PR. For a new RFC, the PR type is `NEW-RFC`.
2. Fill in the compatibility table and the interaction table in the RFC
   itself. Don't leave them for reviewers to ask about. Missing interactions
   and an unspecified lifecycle caused most of the long reviews (4.2).
3. Fix every BLOCKING and IMPORTANT finding. Anything you can't resolve goes
   under "Open questions" in the RFC.
4. Put the summary block from 4.4 in the PR description, plus any findings
   still open, so reviewers can see what was checked.
