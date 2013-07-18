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

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.parsing.CompositeComponentDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.couchbase.monitor.ClientInfo;
import org.springframework.data.couchbase.monitor.ClusterInfo;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * Enables Parsing of the "<couchbase:jmx />" configuration bean.
 *
 * In order to enable JMX, different JmxComponents need to be registered. The dependency to the original
 * {@link com.couchbase.client.CouchbaseClient} object is solved by through the "couchbase-ref" attribute.
 *
 * @author Michael Nitschinger
 */
public class CouchbaseJmxParser implements BeanDefinitionParser {

  /**
   * Parse the element and dispatch the registration of the JMX components.
   *
   * @param element the XML element which contains the attributes.
   * @param parserContext encapsulates the parsing state and configuration.
   * @return null, because no bean instance needs to be returned.
   */
  public BeanDefinition parse(final Element element, final ParserContext parserContext) {
    String name = element.getAttribute("couchbase-ref");
    if (!StringUtils.hasText(name)) {
      name = BeanNames.COUCHBASE;
    }
    registerJmxComponents(name, element, parserContext);
    return null;
  }

  /**
   * Register the JMX components in the context.
   *
   * @param refName the reference name to the couchbase client.
   * @param element the XML element which contains the attributes.
   * @param parserContext encapsulates the parsing state and configuration.
   */
  protected void registerJmxComponents(final String refName, final Element element, final ParserContext parserContext) {
    Object eleSource = parserContext.extractSource(element);
    CompositeComponentDefinition compositeDef = new CompositeComponentDefinition(element.getTagName(), eleSource);

    createBeanDefEntry(ClientInfo.class, compositeDef, refName, eleSource, parserContext);
    createBeanDefEntry(ClusterInfo.class, compositeDef, refName, eleSource, parserContext);

    parserContext.registerComponent(compositeDef);
  }

  /**
   * Creates Bean Definitions for JMX components and adds them as a nested component.
   *
   * @param clazz the class type to register.
   * @param compositeDef component that can hold nested components.
   * @param refName the reference name to the couchbase client.
   * @param eleSource source element to reference.
   * @param parserContext encapsulates the parsing state and configuration.
   */
  protected void createBeanDefEntry(final Class<?> clazz, final CompositeComponentDefinition compositeDef,
    final String refName, final Object eleSource, final ParserContext parserContext) {
    BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(clazz);
    builder.getRawBeanDefinition().setSource(eleSource);
    builder.addConstructorArgReference(refName);
    BeanDefinition assertDef = builder.getBeanDefinition();
    String assertName = parserContext.getReaderContext().registerWithGeneratedName(assertDef);
    compositeDef.addNestedComponent(new BeanComponentDefinition(assertDef, assertName));
  }

}
