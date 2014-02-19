/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.config;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;

import static org.junit.Assert.assertEquals;

/**
 * @author Michael Nitschinger
 */
public class CouchbaseTemplateParserIntegrationTests {

  DefaultListableBeanFactory factory;
  BeanDefinitionReader reader;

  @Before
  public void setUp() {
    factory = new DefaultListableBeanFactory();
    reader = new XmlBeanDefinitionReader(factory);
  }

  @Test
  public void readsCouchbaseTemplateAttributesCorrectly() {
    reader.loadBeanDefinitions(new ClassPathResource("namespace/couchbase-template-bean.xml"));

    BeanDefinition definition = factory.getBeanDefinition("couchbaseTemplate");
    assertEquals(1, definition.getConstructorArgumentValues().getArgumentCount());

    factory.getBean("couchbaseTemplate");
  }

  @Test
  public void readsCouchbaseTemplateWithTranslationServiceAttributesCorrectly() {
    reader.loadBeanDefinitions(new ClassPathResource("namespace/couchbase-template-with-translation-service-bean.xml"));

    BeanDefinition definition = factory.getBeanDefinition("couchbaseTemplate");
    assertEquals(2, definition.getConstructorArgumentValues().getArgumentCount());

    factory.getBean("couchbaseTemplate");
  }

  /**
   * Test case for DATACOUCH-47.
   */
  @Test
  public void allowsMultipleBuckets() {
    reader.loadBeanDefinitions(new ClassPathResource("namespace/couchbase-multi-bucket-bean.xml"));

    factory.getBean("cb-template-first");
    factory.getBean("cb-template-second");
  }

}
