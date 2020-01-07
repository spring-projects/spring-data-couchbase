/*
 * Copyright 2012-2020 the original author or authors
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

import org.junit.Test;
import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.ClassPathResource;

import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

/**
 * @author Simon Bland
 */
public class CouchbaseSingleEnvironmentParserTest {

  /**
   * See DATACOUCH-235
   */
  @Test
  public void testSingleCouchbaseEnvironment() throws Exception {

    int instanceCounterBefore = DefaultCoreEnvironment.instanceCounter();

    DefaultListableBeanFactory factory = new DefaultListableBeanFactory();
    BeanDefinitionReader reader = new XmlBeanDefinitionReader(factory);
    reader.loadBeanDefinitions(new ClassPathResource("configurations/couchbaseSingleEnv-bean.xml"));
    GenericApplicationContext context = new GenericApplicationContext(factory);
    context.refresh();
    CouchbaseEnvironment env = context.getBean("singleEnv", CouchbaseEnvironment.class);
    context.close();

    int instanceCounterAfter = DefaultCoreEnvironment.instanceCounter();

    assertThat(env, is(instanceOf(DefaultCouchbaseEnvironment.class)));
    assertThat(instanceCounterAfter, is(instanceCounterBefore + 1));
  }
}
