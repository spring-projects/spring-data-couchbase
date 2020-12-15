#!/bin/bash -x

set -euo pipefail

mkdir -p /tmp/jenkins-home/.m2/spring-data-couchbase
chown -R 1001:1001 .

MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" \
  ./mvnw \
  -P${PROFILE} clean dependency:list test -Dsort -Dbundlor.enabled=false -U -B -Dmaven.repo.local=/tmp/jenkins-home/.m2/spring-data-couchbase
