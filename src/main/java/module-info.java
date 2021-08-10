module spring.data.couchbase {
  requires spring.tx;
  requires com.couchbase.client.core;
  requires com.couchbase.client.java;
  requires spring.data.commons;
  requires spring.beans;
  requires java.xml;
  requires spring.context;
  requires spring.core;
  requires reactor.core;
  requires slf4j.api;
  requires spring.expression;
  requires org.reactivestreams;
  requires validation.api;
  requires spring.aop;
  requires java.desktop;
  requires com.fasterxml.jackson.databind;
  requires spring.context.support;
  requires org.joda.time;

  // mvn integration-test > opens.log 2>&2
  // awk -F'"' '/does not/ {print $2 ";"}' opens.log | sort -u

  opens org.springframework.data.couchbase.core.mapping;
  opens org.springframework.data.couchbase.core.convert.translation;
  opens org.springframework.data.couchbase.core.query;
  opens org.springframework.data.couchbase.domain;
  opens org.springframework.data.couchbase.repository.query;
  opens org.springframework.data.couchbase.util;
  opens org.springframework.data.couchbase.repository;
  opens org.springframework.data.couchbase.core;

  opens org.springframework.data.couchbase.config to spring.core;
  opens org.springframework.data.couchbase.repository.auditing to spring.core;


  exports org.springframework.data.couchbase.repository.config to spring.beans, spring.core;
  exports org.springframework.data.couchbase.repository;
  exports org.springframework.data.couchbase.config to spring.beans;
  exports org.springframework.data.couchbase.repository.support to spring.beans, spring.data.commons, spring.aop;
  exports org.springframework.data.couchbase.repository.auditing to spring.core, spring.beans;
  exports org.springframework.data.couchbase.core.mapping.event to spring.beans, spring.core;
  exports org.springframework.data.couchbase.core.mapping.id to spring.core; // comment out to reproduce NPE in validateAnnotation
  exports org.springframework.data.couchbase.core.index to spring.core;

}
