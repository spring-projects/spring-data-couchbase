/*
 * Copyright 2012-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.config;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.ClassPathResource;

public class CouchbaseEnvironmentParserTest {

  private static GenericApplicationContext context;

  @BeforeClass
  public static void setUp() {
    DefaultListableBeanFactory factory = new DefaultListableBeanFactory();
    BeanDefinitionReader reader = new XmlBeanDefinitionReader(factory);
    reader.loadBeanDefinitions(new ClassPathResource("configurations/couchbaseEnv-bean.xml"));
    context = new GenericApplicationContext(factory);
    context.refresh();
  }

  @Test
  public void testParsingRetryStrategyFailFast() throws Exception {
    CouchbaseEnvironment env = context.getBean("envWithFailFast", CouchbaseEnvironment.class);

    assertThat(env.retryStrategy(), is(instanceOf(FailFastRetryStrategy.class)));
  }

  @Test
  public void testParsingRetryStrategyBestEffort() throws Exception {
    CouchbaseEnvironment env = context.getBean("envWithBestEffort", CouchbaseEnvironment.class);

    assertThat(env.retryStrategy(), is(instanceOf(BestEffortRetryStrategy.class)));
  }

  @Test
  public void testAllDefaultsOverridden() {
    CouchbaseEnvironment env = context.getBean("envWithNoDefault", CouchbaseEnvironment.class);
    CouchbaseEnvironment defaultEnv = DefaultCouchbaseEnvironment.create();

    assertThat(env, is(instanceOf(DefaultCouchbaseEnvironment.class)));

    assertThat(env.managementTimeout(), is(equalTo(1L)));
    assertThat(env.queryTimeout(), is(equalTo(2L)));
    assertThat(env.viewTimeout(), is(equalTo(3L)));
    assertThat(env.kvTimeout(), is(equalTo(4L)));
    assertThat(env.connectTimeout(), is(equalTo(5L)));
    assertThat(env.disconnectTimeout(), is(equalTo(6L)));
    assertThat(env.dnsSrvEnabled(), allOf(equalTo(true), not(defaultEnv.dnsSrvEnabled())));

    assertThat(env.dcpEnabled(), allOf(equalTo(true), not(defaultEnv.dcpEnabled())));
    assertThat(env.sslEnabled(), allOf(equalTo(true), not(defaultEnv.sslEnabled())));
    assertThat(env.sslKeystoreFile(), is(equalTo("test")));
    assertThat(env.sslKeystorePassword(), is(equalTo("test")));
    assertThat(env.bootstrapHttpEnabled(), allOf(equalTo(false), not(defaultEnv.bootstrapHttpEnabled())));
    assertThat(env.bootstrapCarrierEnabled(), allOf(equalTo(false), not(defaultEnv.bootstrapCarrierEnabled())));
    assertThat(env.bootstrapHttpDirectPort(), is(equalTo(8)));
    assertThat(env.bootstrapHttpSslPort(), is(equalTo(9)));
    assertThat(env.bootstrapCarrierDirectPort(), is(equalTo(10)));
    assertThat(env.bootstrapCarrierSslPort(), is(equalTo(11)));
    assertThat(env.ioPoolSize(), is(equalTo(12)));
    assertThat(env.computationPoolSize(), is(equalTo(13)));
    assertThat(env.responseBufferSize(), is(equalTo(14)));
    assertThat(env.requestBufferSize(), is(equalTo(15)));
    assertThat(env.kvEndpoints(), is(equalTo(16)));
    assertThat(env.viewEndpoints(), is(equalTo(17)));
    assertThat(env.queryEndpoints(), is(equalTo(18)));
    assertThat(env.retryStrategy(), is(instanceOf(FailFastRetryStrategy.class)));
    assertThat(env.maxRequestLifetime(), is(equalTo(19L)));
    assertThat(env.keepAliveInterval(), is(equalTo(20L)));
    assertThat(env.autoreleaseAfter(), is(equalTo(21L)));
    assertThat(env.bufferPoolingEnabled(), allOf(equalTo(false), not(defaultEnv.bufferPoolingEnabled())));
    assertThat(env.tcpNodelayEnabled(), allOf(equalTo(false), not(defaultEnv.tcpNodelayEnabled())));
    assertThat(env.mutationTokensEnabled(), allOf(equalTo(true), not(defaultEnv.mutationTokensEnabled())));
  }

  @AfterClass
  public static void tearDown() {
    context.close();
  }
}