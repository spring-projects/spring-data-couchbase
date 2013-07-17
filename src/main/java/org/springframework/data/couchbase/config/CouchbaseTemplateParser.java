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

import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * @author Michael Nitschinger
 */
public class CouchbaseTemplateParser extends AbstractSingleBeanDefinitionParser {

  protected String resolveId(final Element element, final AbstractBeanDefinition definition,
    final ParserContext parserContext) {
    String id = super.resolveId(element, definition, parserContext);
    return StringUtils.hasText(id) ? id : BeanNames.COUCHBASE_TEMPLATE;
  }

  @Override
  protected Class getBeanClass(final Element element) {
    return CouchbaseTemplate.class;
  }

  @Override
  protected void doParse(final Element element, final BeanDefinitionBuilder bean) {
    String converterRef = element.getAttribute("converter-ref");
    String dbRef = element.getAttribute("db-ref");

    bean.addConstructorArgReference(StringUtils.hasText(dbRef) ? dbRef : BeanNames.COUCHBASE);

    if (StringUtils.hasText(converterRef)) {
      bean.addConstructorArgReference(converterRef);
    }
  }

}
