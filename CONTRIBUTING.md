We happily welcome contributions to Delta Lake. We use [GitHub Issues](/../../issues/) to track community reported issues and [GitHub Pull Requests ](/../../pulls/) for accepting changes.

# Governance
Delta Lake is an independent open-source project and not controlled by any single company. To emphasize this we joined the [Delta Lake Project](https://community.linuxfoundation.org/delta-lake/) in 2019, which is a sub-project of the Linux Foundation Projects. Within the project, we make decisions based on [these rules](https://delta.io/pdfs/delta-charter.pdf).

Delta Lake is supported by a wide set of developers from over 50 organizations across multiple repositories.  Since 2019, more than 190 developers have contributed to Delta Lake!  The Delta Lake community is growing by leaps and bounds with more than 6000 members in the [Delta Users slack](https://go.delta.io/slack)).

For more information, please refer to the [founding technical charter](https://delta.io/pdfs/delta-charter.pdf).

# Communication
- Before starting work on a major feature, please reach out to us via [GitHub](https://github.com/delta-io/delta/issues), [Slack](https://go.delta.io/slack), [email](https://groups.google.com/g/delta-users), etc. We will make sure no one else is already working on it and ask you to open a GitHub issue.
- A "major feature" is defined as any change that is > 100 LOC altered (not including tests), or changes any user-facing behavior.
- We will use the GitHub issue to discuss the feature and come to agreement.
- This is to prevent your time being wasted, as well as ours.
- The GitHub review process for major features is also important so that organizations with commit access can come to agreement on design.
- If it is appropriate to write a design document, the document must be hosted either in the GitHub tracking issue, or linked to from the issue and hosted in a world-readable location. Examples of design documents include [sample 1](https://docs.google.com/document/d/16S7xoAmXpSax7W1OWYYHo5nZ71t5NvrQ-F79pZF6yb8), [sample 2](https://docs.google.com/document/d/1MJhmW_H7doGWY2oty-I78vciziPzBy_nzuuB-Wv5XQ8), and [sample 3](https://docs.google.com/document/d/19CU4eJuBXOwW7FC58uSqyCbcLTsgvQ5P1zoPOPgUSpI).
- Specifically, if the goal is to add a new extension, please read the extension policy.
- Small patches and bug fixes don't need prior communication. If you have identified a bug and have ways to solve it, please create an [issue](https://github.com/delta-io/delta/issues) or create a [pull request](https://github.com/delta-io/delta/pulls).
- If you have an example code that explains a use case or a feature, create a pull request to post under [examples](https://github.com/delta-io/delta/tree/master/examples). 


# Coding style
We generally follow the [Apache Spark Scala Style Guide](https://spark.apache.org/contributing.html).

# Sign your work
The sign-off is a simple line at the end of the explanation for the patch. Your signature certifies that you wrote the patch or otherwise have the right to pass it on as an open-source patch. The rules are pretty simple: if you can certify the below (from developercertificate.org):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

Then you just add a line to every git commit message:

```
Signed-off-by: Jane Smith <jane.smith@email.com>
Use your real name (sorry, no pseudonyms or anonymous contributions.)
```

If you set your `user.name` and `user.email` git configs, you can sign your commit automatically with `git commit -s`.
