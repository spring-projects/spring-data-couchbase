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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.util.StringUtils;

/**
 * Parser for "<couchbase:template />" bean definitions.
 * <p/>
 * The outcome of this bean definition parser will be a constructed {@link CouchbaseTemplate}.
 *
 * @author Michael Nitschinger
 */
public class CouchbaseTemplateParser extends AbstractSingleBeanDefinitionParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTemplateParser.class);

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
    return StringUtils.hasText(id) ? id : BeanNames.COUCHBASE_TEMPLATE;
  }

  /**
   * Defines the bean class that will be constructed.
   *
   * @param element the XML element which contains the attributes.
   * @return the class type to instantiate.
   */
  @Override
  protected Class getBeanClass(final Element element) {
    return CouchbaseTemplate.class;
  }

  /**
   * Parse the bean definition and build up the bean.
   *
   * @param element the XML element which contains the attributes.
   * @param bean the builder which builds the bean.
   */
  @Override
  protected void doParse(final Element element, final BeanDefinitionBuilder bean) {
    String clusterInfoRef = element.getAttribute("clusterInfo-ref");
    String bucketRef = element.getAttribute("bucket-ref");
    String converterRef = element.getAttribute("converter-ref");
    String translationServiceRef = element.getAttribute("translation-service-ref");

    bean.addConstructorArgReference(StringUtils.hasText(clusterInfoRef) ? clusterInfoRef : BeanNames.COUCHBASE_CLUSTER_INFO);
    bean.addConstructorArgReference(StringUtils.hasText(bucketRef) ? bucketRef : BeanNames.COUCHBASE_BUCKET);

    if (StringUtils.hasText(converterRef)) {
      bean.addConstructorArgReference(converterRef);
    }

    if (StringUtils.hasText(translationServiceRef)) {
      bean.addConstructorArgReference(translationServiceRef);
    }

    String consistencyValue = element.getAttribute("consistency");
    if (consistencyValue != null) {
      try {
        Consistency consistency = Consistency.valueOf(consistencyValue);
        bean.addPropertyValue("defaultConsistency", consistency);
      } catch (IllegalArgumentException e) {
        //bad consistency, leave default and log
        LOGGER.warn("Parsed bad consistency value " + consistencyValue + " in xml template configuration, using default");
      }
    }
  }

}
