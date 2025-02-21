# Redirection
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/3702**
<!-- Remove this: Replace XXXX with the actual github issue number -->
<!--  Give a general description / context of the protocol change, and remove this comment. -->
This RFC proposes a new kind of features called "redirection" for Delta tables. The feature enables seamless migration of existing Delta tables to different storage locations while minimizing disruption to user workflows. This enhancement aims to reduce downtime and simplify the process of relocating Delta tables.
## Terminology:
* Modern Delta clients: These are Delta clients that are compatible with the latest Delta protocol and support the redirection feature.
* Legacy Delta clients: These refer to Delta clients that do not support the redirection feature, as they are not up-to-date with the latest Delta protocol.
* Redirect Source Table: The source Delta table that is redirected to a new storage location.
* Redirect Destination Table: The Delta table that is created as a redirected table of an existing source table. Any access to the redirect source table would be dispatched to this redirect destination table.
## Overview
The proposal includes two distinct table features:
* **RedirectReaderWriter**: This feature enables both read and write operations to be redirected from the source to the destination for Modern Delta clients. It blocks all read and write operations from Legacy Delta clients.
* **RedirectWriterOnly**: This feature allows Modern Delta clients to redirect both read and write operations from the source to the destination. However, for Legacy Delta clients, it only blocks write operations while permitting read operations from the source tables.

--------
## Redirection Feature Description
This design includes three sections: feature definition, the procedure of enablement and disablement, and query redirection flow.

### Feature Definition
The Redirection feature provides a redirect type for the Modern Delta clients to parse all other attributes and utilizes the **state** to handle incoming queries.

* **Type**: This is the identifier of the redirection. The Modern Delta clients utilize this value to determine how to parse the redirect specification.

* **State**: This value indicates the status of the redirect table. It has three possible values: **ENABLE-REDIRECT-IN-PROGRESS**, **READY** and **DROP-REDIRECT-IN-PROGRESS**.
	* **ENABLE-REDIRECT-IN-PROGRESS**: This state indicates that the redirect process is still going on. The modern delta client can still read the table on the redirect source table but can't write data to the table. This state also accepts the transaction that updates the value of "delta.redirectReaderWriter" and "delta.redirectWriterOnly".
	* **READY**: This state indicates that the redirect process is completed and the modern delta client can read and write the table. Both table-based and path-based read and write statements are redirected to the destination table.
	* **DROP-REDIRECT-IN-PROGRESS**: The table redirection is under withdrawal and the redirection property is going to be removed from the delta table. In this state, the modern delta client stops redirecting new queries to redirect destination tables, and only accepts read-only queries to access the redirect source table. The on-going redirected write or metadata transactions, which are visiting redirect destinations, can not commit. This state accepts the transaction that updates the value of "delta.redirectReaderWriter" and "delta.redirectWriterOnly".
	* Concurrent write transactions are aborted using the existing conflict checker mechanism. Therefore, state transitions should commit to both the redirect source and destination tables. This ensures that the Delta table's conflict checker can automatically manage concurrent write transactions.

* **Spec**: It is a free form JSON object that describes the information of accessing the redirected destination table. Its value is determined by the Type attribute, so each delta vendor should implement their own implementation of this JSON object. We describe the JSON definitions of a Unity Catalog table and an object store table without any catalog below as examples.
	* Unity Catalog Table:
		* Table: It is a JSON to get the table information of the redirected table. This attribute's value type depends on the Type above. It can be UUID, integer or <database>.<schema>.<name> triple.
```
	delta.redirectReaderWriter: "{
		"Type": "Unity Catalog",
		"State": "READY",
		"Spec": "{
			"Table": "{
				"MetastoreId": "c535492e-375f-4b34-887c-f309ed83bf8d",
				"TableId": "fba84a4d-8789-44db-ae04-5203fbed599b"
			}",
		}",
	}"
```
	* Object Store Table Without Catalog:
		* Location: The URI of the object store path that contains the delta log files of the redirect destination table.
```
	delta.redirectReaderWriter: "{
		"Type": "Object Store",
		"State": "READY",
		"Spec": "{
			"Location": "s3://........./"
		}"
	}"
```

* **NoRedirectRules**: This attribute contains a list of rules for allowing transactions on the redirect source table. It allows maintenance workloads to run on the redirect source tables, for instance, the REFRESH UNIFORM and VACUUM command on the redirect source table. Each rule includes two attributes:
	* **AppName**: This is the name of the applications that are allowed to execute commands on the redirect source table. When the sessions with these AppName try to access the redirected table, the modern delta client disables table redirection and their queries would be run on the redirect source table.
	* **AllowWrite**: This field contains the list of delta write operations that are allowed to execute on the redirect source table.

### Enable & Disable Procedure
This section describes the procedures to enable and disable table redirection. By following these procedures, all Delta clients can safely point their redirect source table to the redirect destination table, or disassociate the redirection to only work on their original redirect source table directly.

#### Enable Procedure
The enablement procedure includes three states: **NORMAL**, **ENABLE-REDIRECT-IN-PROGRESS** and **READY**. These states are divided by two commits on the redirect source table. The **NORMAL** state represents a table that doesn’t have the redirect table feature yet.

##### From NORMAL to ENABLE-REDIRECT-IN-PROGRESS
When a table redirect command starts, the modern delta client issues the first commit to move the redirect source table from NORMAL to ENABLE-REDIRECT-IN-PROGRESS state. This transaction creates the redirect table feature entry inside the Deltalog of the redirect source table. This entry should provide: Type and State. The State should be ENABLE-REDIRECT-IN-PROGRESS, which blocks all subsequent write transactions and allows read only queries.
```
	delta.redirectReaderWriter: "{
		"Type": "UNITY CATALOG",
		"State": "ENABLE-REDIRECT-IN-PROGRESS",
		"Spec": "{}",
	}"
```

After entering the ENABLE-REDIRECT-IN-PROGRESS, the modern delta client coordinates with the destination storage to complete migration work. Beside storage setup, all existing commit logs, checkpoints and checksums of the redirect source table should be copied to the redirect destination table’s storage location.

##### From ENABLE-REDIRECT-IN-PROGRESS to READY
After completing the migration setup above, the modern delta client makes the second commit to finalize the redirect feature on the redirect source table. This commit provides the Table, which is used by catalog to lookup the redirect destination table. The State should be set to READY that indicates the redirect source table is ready to route all subsequential workloads to its redirect destination table.
```
        delta.redirectReaderWriter: "{
                "Type": "UNITY CATALOG",
                "State": "READY",
                "Spec": "{}",
        }"
```
#### Disable Procedure
The disablement procedure has three states: READY, DROP-REDIRECT-IN-PROGRESS and NORMAL, which are separated by two commits that update the redirect feature's property.

##### READY to DROP-REDIRECT-IN-PROGRESS
When a user starts canceling the redirection, the modern delta client issues the first commit, which updates the value of "delta.redirectReaderWriter" property, to move both the redirect source and destination table from READY to DROP-REDIRECT-IN-PROGRESS.
```
	delta.redirectReaderWriter: "{
		"Type": "UNITY CATALOG",
		"State": "DROP-REDIRECT-IN-PROGRESS",
		"Spec": "",
	}"
```
In this state, all subsequent write and metadata change transactions are aborted. Read-only queries may either be processed by the redirect destination table or blocked, depending on the implementation of the destination storage. The modern Delta client then collaborates with the destination storage to clean up resources and clone the commit logs, checkpoints, and checksum files from the redirect destination table back to the redirect source table.


##### DROP-REDIRECT-IN-PROGRESS to NORMAL
Once resource cleanup is complete and all files are cloned, the modern Delta client makes a second transaction commit to remove the "delta.redirectReaderWriter" property from the Delta log, transitioning the redirect source table from DROP-REDIRECT-IN-PROGRESS to NORMAL. This disassociates the redirect source table from the redirect destination table, allowing it to handle all types of transactions directly. Consequently, the redirect source table resumes processing all transactions, and the redirect destination table is no longer accessible.

### Query Redirection Flow
When the redirect source table reaches the READY state, the modern Delta client routes all queries accessing this table to the corresponding redirect destination table, utilizing the TableId and Type information.

#### Redirect Read Query
When a modern delta client receives a read query that access a table with redirected property, the modern delta client handles this query in following steps. If the state is ENABLE-REDIRECT-IN-PROGRESS, the modern delta client uses the delta snapshot of the redirect source snapshot. If the state is READY or DROP-REDIRECT-IN-PROGRESS, the modern delta client constructs the delta snapshot from the redirect destination table.

#### Redirect Write Query
When a modern Delta client receives a write query for a Delta table with redirection enabled, it retrieves the redirect destination table's information in the same manner as described for Redirect Read Queries. The Delta client then writes all subsequent Parquet data files, commit JSON files, checkpoint files, and checksum files to the storage location of the redirect destination table.

<!-- Remove this: Add your proposed protocol.md modifications here, and remove this comment. -->
