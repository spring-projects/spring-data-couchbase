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

import org.w3c.dom.Element;

import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.util.StringUtils;

/**
 * The XML parser for a {@link IndexManager} definition.
 *
 * @author Simon Baslé
 */
public class CouchbaseIndexManagerParser extends AbstractSingleBeanDefinitionParser {

  /**
   * Resolve the bean ID and assign a default if not set.
   *
   * @param element the XML element which contains the attributes.
   * @param definition the bean definition to work with.
   * @param parserContext encapsulates the parsing state and configuration.
   * @return the ID to work with.
   */
  @Override
  protected String resolveId(final Element element, final AbstractBeanDefinition definition, final ParserContext parserContext) {
    String id = super.resolveId(element, definition, parserContext);
    return StringUtils.hasText(id) ? id : BeanNames.COUCHBASE_INDEX_MANAGER;
  }

  /**
   * Defines the bean class that will be constructed.
   *
   * @param element the XML element which contains the attributes.
   * @return the class type to instantiate.
   */
  @Override
  protected Class getBeanClass(final Element element) {
    return IndexManager.class;
  }

  /**
   * Parse the bean definition and build up the bean.
   *
   * @param element the XML element which contains the attributes.
   * @param bean the builder which builds the bean.
   */
  @Override
  protected void doParse(final Element element, final BeanDefinitionBuilder bean) {
    boolean ignoreViews = Boolean.parseBoolean(element.getAttribute("ignoreViews"));
    boolean ignorePrimary = Boolean.parseBoolean(element.getAttribute("ignorePrimary"));
    boolean ignoreSecondary = Boolean.parseBoolean(element.getAttribute("ignoreSecondary"));

    bean.addConstructorArgValue(ignoreViews);
    bean.addConstructorArgValue(ignorePrimary);
    bean.addConstructorArgValue(ignoreSecondary);
  }

}
