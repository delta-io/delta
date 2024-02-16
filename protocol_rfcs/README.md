# Protocol RFCs

This directory contains information about the process of making Delta protocol changes via RFCs and all the RFCs that have been proposed since
 this process was adopted.
 
 - [Table of RFCs](#table-of-rfcs)
    - [Proposed RFCs](#proposed-rfcs)
    - [Accepted RFCs](#accepted-rfcs)
    - [Rejected RFCs](#rejected-rfcs)
 - [RFC Process](#rfc-process)


## Table of RFCs

Here is the history of all the RFCs propose/accepted/rejected since Feb 6, 2024, when this process was introduced.

### Proposed RFCs

| Date proposed | RFC file | Github issue | RFC title |
|:-|:-|:-|:-|
| 2023-02-02 | [in-commit-timestamps.md](https://github.com/delta-io/delta/blob/master/protocol_rfcs/in-commit-timestamps.md) | https://github.com/delta-io/delta/issues/2532 | In-Commit Timestamps |

### Accepted RFCs

| Date proposed | Date accepted | RFC file | Github issue | RFC title |
|:-|:-|:-|:-|:-|
|...|||||

### Rejected RFCs

| Date proposed | Date rejected | RFC file | Github issue | RFC title |
|:-|:-|:-|:-|:-|
|...|||||


## RFC process

###  **1. Make initial proposal** 
Create a Github issue of type [Protocol Change Request].
- The description of the issue may have links to design docs, etc.
- This issue will serve as the central location for all discussions related to the protocol change.
- If the proposal comes with a prototype or other pathfinding, the changes should be in an open PR. 

### **2. Add the RFC doc** 
After creating the issue and discussing with the community, if a basic consensus is reached that this feature should be implemented, then create a PR to add the protocol RFC before merging code in master.
- Clone the RFC template `template.md` and create a new RFC markdown doc.
- Cross-link with the issue with "see #xxx". DONT USE "closes #xxx" or "fixes #xxx" or "resolves #xxx" because we don't want the issue to be closed when this RFC PR is merged.

Note:
- For table features, it is strongly recommended that any experimental support for the feature uses a temporary feature name with a suffix like `-dev`. This will communicate to the users that are about to use experimental feature with no future compatibility guarantee.
- Code related to a proposed feature should not be merged into the main branch until the RFC attains "proposed" status (that is, the RFC PR has been through public review and merged). Until the RFC has been accepted (that is, the proposed changes have been merged into the Delta specification), any code changes should be isolated from production code behind feature flags, etc. so that existing users are not affected in any way.

###  **3. Finally, accept or reject the RFC** 
For a RFC to be accepted, it must satisfy the following criteria:
- There is a production implementation (for example, in delta-spark) of the feature that has been thoroughly well tested.
- There is at least some discussion and/or prototype (preferred) that ensure the feasibility of the feature in Delta Kernel. 

When the success criteria are met, then the protocol can be finalized by making a PR to make the following changes:
-  Closely validate that the protocol spec changes are actually consistent with the production implementation.
-  Cross-link the PR with the original issue with "closes #xxx" as now we are ready to close the issue. In addition, update the title of the issue to say `[ACCEPTED]` to make it obvious how the proposal was resolved.
-  Update `protocol.md`.
-  Move the RFC doc to the `accepted` subdirectory, and update the state in index.md.
-  Remove the temporary/preview suffix like `-dev` in the table feature name from all the code. 

However, if the RFC is to be rejected, then make a PR to do the following changes:
- Cross-link the PR with the original issue with "closes #xxx" as now we are ready to close the issue. In addition, update the title of the issue to say `[REJECTED]` to make it obvious how the proposal was resolved.
 - Move the RFC doc to the `rejected` subdirectory.
 - Update the state in `index.md`.
 - Remove any experimental/preview code related to the feature.
