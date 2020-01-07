/*
 * Copyright 2012-2019 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.config;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.couchbase.client.java.env.ClusterEnvironment;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.ClassPathResource;

import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.FailFastRetryStrategy;

import java.time.Duration;


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

  /*@Test
  public void testParsingRetryStrategyFailFast() throws Exception {
    ClusterEnvironment env = context.getBean("envWithFailFast", ClusterEnvironment.class);

    assertThat(env.retryStrategy(), is(instanceOf(FailFastRetryStrategy.class)));
  }

  @Test
  public void testParsingRetryStrategyBestEffort() throws Exception {
    ClusterEnvironment env = context.getBean("envWithBestEffort", ClusterEnvironment.class);

    assertThat(env.retryStrategy(), is(instanceOf(BestEffortRetryStrategy.class)));
  }

  @Test
  public void testAllDefaultsOverridden() {
    ClusterEnvironment env = context.getBean("envWithNoDefault", ClusterEnvironment.class);
    ClusterEnvironment defaultEnv = ClusterEnvironment.create();

    assertThat(env.timeoutConfig().managementTimeout(), is(equalTo(Duration.ofMillis(1L))));
    assertThat(env.timeoutConfig().queryTimeout(), is(equalTo(Duration.ofMillis(2L))));
    assertThat(env.timeoutConfig().viewTimeout(), is(equalTo(Duration.ofMillis(3L))));
    assertThat(env.timeoutConfig().kvTimeout(), is(equalTo(Duration.ofMillis(4L))));
    assertThat(env.timeoutConfig().connectTimeout(), is(equalTo(Duration.ofMillis(5L))));
    assertThat(env.timeoutConfig().disconnectTimeout(), is(equalTo(Duration.ofMillis(6L))));
    assertThat(env.ioConfig().dnsSrvEnabled(), allOf(equalTo(true), not(defaultEnv.ioConfig().dnsSrvEnabled())));

    assertThat(env.sslEnabled(), allOf(equalTo(true), not(defaultEnv.sslEnabled())));
    assertThat(env.sslKeystoreFile(), is(equalTo("test")));
    assertThat(env.sslKeystorePassword(), is(equalTo("test")));
    assertThat(env.bootstrapHttpEnabled(), allOf(equalTo(false), not(defaultEnv.bootstrapHttpEnabled())));
    assertThat(env.bootstrapCarrierEnabled(), allOf(equalTo(false), not(defaultEnv.bootstrapCarrierEnabled())));
    assertThat(env.bootstrapHttpDirectPort(), is(equalTo(8)));
    assertThat(env.bootstrapHttpSslPort(), is(equalTo(9)));
    assertThat(env.bootstrapCarrierDirectPort(), is(equalTo(10)));
    assertThat(env.bootstrapCarrierSslPort(), is(equalTo(11)));
    assertThat(env.ioConfig().ioPoolSize(), is(equalTo(12)));
    assertThat(env.computationPoolSize(), is(equalTo(13)));
    assertThat(env.responseBufferSize(), is(equalTo(14)));
    assertThat(env.requestBufferSize(), is(equalTo(15)));

    // TODO: look at how to deal with max/min endpoints!
    // assertThat(env.serviceConfig().keyValueServiceConfig().maxEndpoints(), is(equalTo(16)));
    //assertThat(env.serviceConfig().viewServiceConfig().maxEndpoints(), is(equalTo(17)));
    //assertThat(env.serviceConfig().queryServiceConfig().maxEndpoints(), is(equalTo(18)));

    // assertThat(env.retryStrategy(), is(instanceOf(FailFastRetryStrategy.class)));
    assertThat(env.maxRequestLifetime(), is(equalTo(19L)));
    assertThat(env.keepAliveInterval(), is(equalTo(20L)));
    assertThat(env.autoreleaseAfter(), is(equalTo(21L)));
    assertThat(env.bufferPoolingEnabled(), allOf(equalTo(false), not(defaultEnv.bufferPoolingEnabled())));
    assertThat(env.tcpNodelayEnabled(), allOf(equalTo(false), not(defaultEnv.tcpNodelayEnabled())));

    //  assertThat(env.ioConfig().mutationTokensEnabled(), allOf(equalTo(false), not(defaultEnv.ioConfig().mutationTokensEnabled())));
    //  assertThat(env.serviceConfig().analyticsServiceConfig().idleTime(), is(equalTo(Duration.ofMillis(30L))));
    //  assertThat(env.ioConfig().configPollInterval(), is(equalTo(Duration.ofMillis(50L))));
    //assertThat(env.configPollFloorInterval(), is(equalTo(30L)));
    //assertThat(env.operationTracingEnabled(), allOf(equalTo(false), not(defaultEnv.operationTracingEnabled())));
    //assertThat(env.operationTracingServerDurationEnabled(), allOf(equalTo(false), not(defaultEnv.operationTracingServerDurationEnabled())));
    //assertThat(env.orphanResponseReportingEnabled(), allOf(equalTo(false), not(defaultEnv.orphanResponseReportingEnabled())));
    assertThat(env.compressionConfig().minSize(), is(equalTo(100)));
    assertThat(env.compressionConfig().minRatio(), is(equalTo(0.90)));
  }*/

  @AfterClass
  public static void tearDown() {
    context.close();
  }
}
