/*
 * Copyright 2013-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.config;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.couchbase.repository.User;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Michael Nitschinger
 * @author Simon Baslé
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = CouchbaseTemplateParserIntegrationTests.class)
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
    reader.loadBeanDefinitions(new ClassPathResource("configurations/couchbase-template-bean.xml"));

    BeanDefinition definition = factory.getBeanDefinition(BeanNames.COUCHBASE_TEMPLATE);
    assertThat(definition.getConstructorArgumentValues().getArgumentCount()).isEqualTo(2);

    factory.getBean(BeanNames.COUCHBASE_TEMPLATE);
  }

  @Test
  public void readsCouchbaseTemplateWithTranslationServiceAttributesCorrectly() {
    reader.loadBeanDefinitions(new ClassPathResource("configurations/couchbase-template-with-translation-service-bean.xml"));

    BeanDefinition definition = factory.getBeanDefinition(BeanNames.COUCHBASE_TEMPLATE);
    assertThat(definition.getConstructorArgumentValues().getArgumentCount()).isEqualTo(3);

    factory.getBean(BeanNames.COUCHBASE_TEMPLATE);
  }

  /**
   * Test case for DATACOUCH-47.
   */
  @Test
  public void allowsMultipleBuckets() {
    reader.loadBeanDefinitions(new ClassPathResource("configurations/couchbase-multi-bucket-bean.xml"));

    factory.getBean("cb-template-first");
    factory.getBean("cb-template-second");
  }

  /**
   * Test case for DATACOUCH-134 in xml: field for storing type information can be renamed.
   */
  @Test
  public void testTypeFieldCanBeChosen() {
    reader.loadBeanDefinitions(new ClassPathResource("configurations/couchbase-typekey.xml"));
    CouchbaseTemplate template = factory.getBean(BeanNames.COUCHBASE_TEMPLATE, CouchbaseTemplate.class);

    assertThat(template.getConverter() instanceof MappingCouchbaseConverter).isTrue();
    MappingCouchbaseConverter converter = ((MappingCouchbaseConverter) template.getConverter());

    assertThat(converter.getTypeKey()).isEqualTo("javaXmlClass");

    User u = new User("specialSaveUser", "John Locke", 46);
    template.save(u);
    JsonDocument uJsonDoc = template.getCouchbaseBucket().get("specialSaveUser");
    template.getCouchbaseBucket().remove("specialSaveUser");
    assertThat(uJsonDoc).isNotNull();
    JsonObject uJson = uJsonDoc.content();
    assertThat(uJson.get(MappingCouchbaseConverter.TYPEKEY_DEFAULT)).isNull();
    assertThat(uJson.getString("javaXmlClass"))
			.isEqualTo("org.springframework.data.couchbase.repository.User");
    assertThat(uJson.getString("username")).isEqualTo("John Locke");
  }

  /**
   * Test case for DATACOUCH-148, choosing an alternative default for view/N1QL staleness.
   */
  @Test
  public void shouldParseCustomStaleness() {
    reader.loadBeanDefinitions(new ClassPathResource("configurations/couchbase-consistency.xml"));
    CouchbaseTemplate template = factory.getBean("template", CouchbaseTemplate.class);

    assertThat(template.getDefaultConsistency())
			.isEqualTo(Consistency.EVENTUALLY_CONSISTENT);
    assertThat(template.getDefaultConsistency())
			.isNotEqualTo(Consistency.DEFAULT_CONSISTENCY);
  }

  /**
   * Test case for DATACOUCH-148, choosing an unknown value for view/N1QL staleness.
   */
  @Test
  public void shouldIgnoreBadCustomStaleness() {
    reader.loadBeanDefinitions(new ClassPathResource("configurations/couchbase-consistency.xml"));
    CouchbaseTemplate template = factory.getBean("templateBad", CouchbaseTemplate.class);

    assertThat(template.getDefaultConsistency())
			.isEqualTo(Consistency.DEFAULT_CONSISTENCY);
  }

  @Test
  public void shouldHaveDefaultsForStaleness() {
    //use another resource where staleness isn't customized
    reader.loadBeanDefinitions(new ClassPathResource("configurations/couchbase-template-with-translation-service-bean.xml"));
    CouchbaseTemplate template = factory.getBean(BeanNames.COUCHBASE_TEMPLATE, CouchbaseTemplate.class);

    assertThat(template.getDefaultConsistency())
			.isEqualTo(Consistency.DEFAULT_CONSISTENCY);
  }
}
