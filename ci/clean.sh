#!/bin/bash -x

set -euo pipefail
chown -R 1001:1001 .

export JENKINS_USER=${JENKINS_USER_NAME}

MAVEN_OPTS="-Duser.name=${JENKINS_USER} -Duser.home=/tmp/jenkins-home" \
  ./mvnw -s settings.xml clean -Dscan=false -Dmaven.repo.local=/tmp/jenkins-home/.m2/spring-data-couchbase
