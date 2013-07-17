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

import com.couchbase.client.CouchbaseClient;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.couchbase.core.CouchbaseFactoryBean;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;


/**
 * Parser for "<couchbase:couchbase />" definitions.
 *
 * @author Michael Nitschinger
 */
public class CouchbaseParser extends AbstractSingleBeanDefinitionParser {

  @Override
  protected Class getBeanClass(final Element element) {
    return CouchbaseClient.class;
  }

  @Override
  protected void doParse(final Element element, final BeanDefinitionBuilder bean) {
    String host = element.getAttribute("host");
    bean.addConstructorArgValue(
      convertHosts(StringUtils.hasText(host) ? host : CouchbaseFactoryBean.DEFAULT_NODE));
    String bucket = element.getAttribute("bucket");
    bean.addConstructorArgValue(
      StringUtils.hasText(bucket) ? bucket : CouchbaseFactoryBean.DEFAULT_BUCKET);
    String password = element.getAttribute("password");
    bean.addConstructorArgValue(
      StringUtils.hasText(password) ? password : CouchbaseFactoryBean.DEFAULT_PASSWORD);
  }

  protected String resolveId(final Element element, final AbstractBeanDefinition definition,
    final ParserContext parserContext) {
    String id = super.resolveId(element, definition, parserContext);
    return StringUtils.hasText(id) ? id : BeanNames.COUCHBASE;
  }

  private List<URI> convertHosts(final String hosts) {
    String[] split = hosts.split(",");
    List<URI> nodes = new ArrayList<URI>();

    try {
      for (int i = 0; i < split.length; i++) {
        nodes.add(new URI("http://" + split[i] + ":8091/pools"));
      }
    } catch (URISyntaxException ex) {
      throw new BeanCreationException("Could not convert host list." + ex);
    }

    return nodes;
  }

}
