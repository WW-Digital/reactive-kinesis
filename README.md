# reactive-kinesis [![Build Status](https://travis-ci.org/WW-Digital/reactive-kinesis.svg?branch=master)](https://travis-ci.org/WW-Digital/reactive-kinesis) [![Coverage Status](https://coveralls.io/repos/github/WW-Digital/reactive-kinesis/badge.svg?branch=master)](https://coveralls.io/github/WW-Digital/reactive-kinesis?branch=master)

Welcome to reactive-kinesis!

## Tag Requirements
Uses tags and [sbt-git](https://github.com/sbt/sbt-git) to determine the current version.

* IF the current commit is tagged with "vX.Y.Z" (ie semantic-versioning), the version is "X.Y.Z"
* ELSE IF the current commit is tagged with "vX.Y.Z-Mx", the version is "X.Y.Z-Mx"
* ELSE IF the current commit is tagged with "vX.Y.Z-SNAPSHOT", the version is "X.Y.Z-commitsSinceVersion-SNAPSHOT"
* ELSE IF the latest found tag is "vX.Y.Z", the version is "X.Y.Z-commitsSinceVersion-gCommitHash-SNAPSHOT"
* ELSE the version is "0.0.0-commitHash-SNAPSHOT"

### Valid Release Tag Examples:
v1.2.3 (version=1.2.3)
v1.2.3-M1 (version=1.2.3-M1)

### Invalid Release Tag Examples:
v1.2.3-SNAPSHOT
v1.2.3-M1-SNAPSHOT
v1.2.3-X1
1.2.3

If the current version on master is a snapshot (release tag + x commits), 
then the artifact will be deployed to the [JFrog OSS repository](https://oss.jfrog.org/webapp/#/artifacts/browse/simple/General/oss-snapshot-local/com/weightwatchers): 

## Contribution policy ##

Contributions via GitHub pull requests are gladly accepted from their original author. Along with
any pull requests, please state that the contribution is your original work and that you license
the work to the project under the project's open source license. Whether or not you state this
explicitly, by submitting any copyrighted material via pull request, email, or other means you
agree to license the material under the project's open source license and warrant that you have the
legal authority to do so.

## License ##

This code is open source software licensed under the
[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0) license.
