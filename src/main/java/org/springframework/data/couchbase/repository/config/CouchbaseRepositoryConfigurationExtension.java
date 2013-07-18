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

package org.springframework.data.couchbase.repository.config;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.data.config.ParsingUtils;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactoryBean;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.XmlRepositoryConfigurationSource;
import org.w3c.dom.Element;

/**
 * @author Michael Nitschinger
 */
public class CouchbaseRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport {

  private static final String COUCHBASE_TEMPLATE_REF = "couchbase-template-ref";

  @Override
  protected String getModulePrefix() {
    return "couchbase";
  }

  public String getRepositoryFactoryClassName() {
    return CouchbaseRepositoryFactoryBean.class.getName();
  }

  @Override
  public void postProcess(final BeanDefinitionBuilder builder, final XmlRepositoryConfigurationSource config) {
    Element element = config.getElement();
    ParsingUtils.setPropertyReference(builder, element, COUCHBASE_TEMPLATE_REF, "couchbaseOperations");
  }

  @Override
  public void postProcess(final BeanDefinitionBuilder builder, final AnnotationRepositoryConfigurationSource config) {
    AnnotationAttributes attributes = config.getAttributes();
    builder.addPropertyReference("couchbaseOperations", attributes.getString("couchbaseTemplateRef"));
  }
}
