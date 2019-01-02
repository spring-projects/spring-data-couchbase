/*
 * Copyright 2012-2019 the original author or authors
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

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.cluster.ClusterInfo;
import org.w3c.dom.Element;

import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;

/**
 * The parser for XML definition of a {@link ClusterInfo}, to be constructed from a {@link Cluster} reference.
 * If no reference is given, the default reference <code>{@value BeanNames#COUCHBASE_CLUSTER_INFO}</code> is used.
 * <p/>
 * See attributes {@link #CLUSTER_REF_ATTR}, {@link #LOGIN_ATTR} and {@link #PASSWORD_ATTR}.
 *
 * @author Simon Baslé
 */
public class CouchbaseClusterInfoParser extends AbstractSingleBeanDefinitionParser {

  /**
   * The <code>cluster-ref</code> attribute in a cluster info definition defines the cluster to build from.
   */
  public static final String CLUSTER_REF_ATTR = "cluster-ref";

  /**
   * The <code>login</code> attribute in a cluster info definition defines the credential to use (can also be a
   * bucket level credential).
   */
  public static final String LOGIN_ATTR = "login";

  /**
   * The <code>password</code> attribute in a cluster info definition defines the credential's password.
   */
  public static final String PASSWORD_ATTR = "password";

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
    return StringUtils.hasText(id) ? id : BeanNames.COUCHBASE_CLUSTER_INFO;
  }

  /**
   * Defines the bean class that will be constructed.
   *
   * @param element the XML element which contains the attributes.
   * @return the class type to instantiate.
   */
  @Override
  protected Class getBeanClass(final Element element) {
    return CouchbaseClusterInfoFactoryBean.class;
  }

  /**
   * Parse the bean definition and build up the bean.
   *
   * @param element the XML element which contains the attributes.
   * @param builder the builder which builds the bean.
   */
  @Override
  protected void doParse(final Element element, final BeanDefinitionBuilder builder) {
    String clusterRef = element.getAttribute(CLUSTER_REF_ATTR);
    if (!StringUtils.hasText(clusterRef)) {
      clusterRef = BeanNames.COUCHBASE_CLUSTER;
    }
    builder.addConstructorArgReference(clusterRef);

    String login = element.getAttribute(LOGIN_ATTR);
    if (!StringUtils.hasText(login)) {
      login = "default";
    }
    builder.addConstructorArgValue(login);

    String password = element.getAttribute(PASSWORD_ATTR);
    if (!StringUtils.hasText(password)) {
      password = "";
    }
    builder.addConstructorArgValue(password);
  }
}
