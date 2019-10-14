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
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import static org.assertj.core.api.Assertions.assertThat;

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

    assertThat(env.retryStrategy()).isInstanceOf(FailFastRetryStrategy.class);
  }

  @Test
  public void testParsingRetryStrategyBestEffort() throws Exception {
    CouchbaseEnvironment env = context.getBean("envWithBestEffort", CouchbaseEnvironment.class);

    assertThat(env.retryStrategy()).isInstanceOf(BestEffortRetryStrategy.class);
  }

  @Test
  public void testAllDefaultsOverridden() {
    CouchbaseEnvironment env = context.getBean("envWithNoDefault", CouchbaseEnvironment.class);
    CouchbaseEnvironment defaultEnv = DefaultCouchbaseEnvironment.create();

    assertThat(env).isInstanceOf(DefaultCouchbaseEnvironment.class);

    assertThat(env.managementTimeout()).isEqualTo(1L);
    assertThat(env.queryTimeout()).isEqualTo(2L);
    assertThat(env.viewTimeout()).isEqualTo(3L);
    assertThat(env.kvTimeout()).isEqualTo(4L);
    assertThat(env.connectTimeout()).isEqualTo(5L);
    assertThat(env.disconnectTimeout()).isEqualTo(6L);
    assertThat(env.dnsSrvEnabled()).isTrue().isNotEqualTo(defaultEnv.dnsSrvEnabled());

    assertThat(env.sslEnabled()).isTrue().isNotEqualTo(defaultEnv.sslEnabled());
    assertThat(env.sslKeystoreFile()).isEqualTo("test");
    assertThat(env.sslKeystorePassword()).isEqualTo("test");
    assertThat(env.bootstrapHttpEnabled()).isFalse()
			.isNotEqualTo(defaultEnv.bootstrapHttpEnabled());
    assertThat(env.bootstrapCarrierEnabled()).isFalse()
			.isNotEqualTo(defaultEnv.bootstrapCarrierEnabled());
    assertThat(env.bootstrapHttpDirectPort()).isEqualTo(8);
    assertThat(env.bootstrapHttpSslPort()).isEqualTo(9);
    assertThat(env.bootstrapCarrierDirectPort()).isEqualTo(10);
    assertThat(env.bootstrapCarrierSslPort()).isEqualTo(11);
    assertThat(env.ioPoolSize()).isEqualTo(12);
    assertThat(env.computationPoolSize()).isEqualTo(13);
    assertThat(env.responseBufferSize()).isEqualTo(14);
    assertThat(env.requestBufferSize()).isEqualTo(15);
    assertThat(env.kvEndpoints()).isEqualTo(16);
    assertThat(env.viewEndpoints()).isEqualTo(17);
    assertThat(env.queryEndpoints()).isEqualTo(18);
    assertThat(env.retryStrategy()).isInstanceOf(FailFastRetryStrategy.class);
    assertThat(env.maxRequestLifetime()).isEqualTo(19L);
    assertThat(env.keepAliveInterval()).isEqualTo(20L);
    assertThat(env.autoreleaseAfter()).isEqualTo(21L);
    assertThat(env.bufferPoolingEnabled()).isFalse()
			.isNotEqualTo(defaultEnv.bufferPoolingEnabled());
    assertThat(env.tcpNodelayEnabled()).isFalse()
			.isNotEqualTo(defaultEnv.tcpNodelayEnabled());
    assertThat(env.mutationTokensEnabled()).isTrue()
			.isNotEqualTo(defaultEnv.mutationTokensEnabled());
    assertThat(env.analyticsTimeout()).isEqualTo(30L);
    assertThat(env.configPollInterval()).isEqualTo(50L);
    assertThat(env.configPollFloorInterval()).isEqualTo(30L);
    assertThat(env.operationTracingEnabled()).isFalse()
			.isNotEqualTo(defaultEnv.operationTracingEnabled());
    assertThat(env.operationTracingServerDurationEnabled()).isFalse()
			.isNotEqualTo(defaultEnv.operationTracingServerDurationEnabled());
    assertThat(env.orphanResponseReportingEnabled()).isFalse()
			.isNotEqualTo(defaultEnv.orphanResponseReportingEnabled());
    assertThat(env.compressionMinSize()).isEqualTo(100);
    assertThat(env.compressionMinRatio()).isEqualTo(0.90);
  }

  @AfterClass
  public static void tearDown() {
    context.close();
  }
}
