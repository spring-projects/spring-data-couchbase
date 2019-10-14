package org.springframework.data.couchbase.repository.xmlconfig;

import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
@SuppressWarnings("SpringJavaAutowiringInspection")
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = XmlRepositoryConfigurationIntegrationTests.class)
public class XmlRepositoryConfigurationIntegrationTests {

  DefaultListableBeanFactory factory;
  BeanDefinitionReader reader;

  @Before
  public void setUp() {
    factory = new DefaultListableBeanFactory();
    reader = new XmlBeanDefinitionReader(factory);
  }

  @Test
  public void testInstantiateRepositoryFromXml() {
    reader.loadBeanDefinitions(new ClassPathResource("configurations/couchbase-repository-bean.xml"));

    BeanDefinition definition = factory.getBeanDefinition("xmlItemRepository");
    assertThat(definition.getBeanClassName())
			.isEqualTo("org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactoryBean");
    assertDefinitionProperty(definition, "couchbaseOperations");

    Object bean = factory.getBean("xmlItemRepository");
    assertThat(bean instanceof XmlItemRepository).isTrue();
  }

  private void assertDefinitionProperty(BeanDefinition definition, String property) {
    assertThat(definition.getPropertyValues().contains(property))
			.as("bean definition properties don't include " + property).isTrue();
  }
}
