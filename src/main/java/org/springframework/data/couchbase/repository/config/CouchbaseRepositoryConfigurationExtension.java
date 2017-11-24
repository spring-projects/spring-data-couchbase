/*
 * Copyright 2013-2017 the original author or authors.
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

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.data.config.ParsingUtils;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactoryBean;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.XmlRepositoryConfigurationSource;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.w3c.dom.Element;

/**
 * @author Michael Nitschinger
 * @author Mark Paluch
 * @author Oliver Gierke
 */
public class CouchbaseRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport {

  /** The reference property to use in xml configuration to specify the template to use with a repository. */
  private static final String COUCHBASE_TEMPLATE_REF = "couchbase-template-ref";

  /** The reference property to use in xml configuration to specify the index manager bean to use with a repository. */
  private static final String COUCHBASE_INDEX_MANAGER_REF = "couchbase-index-manager-ref";

  @Override
  protected String getModulePrefix() {
    return "couchbase";
  }

  public String getRepositoryFactoryBeanClassName() {
    return CouchbaseRepositoryFactoryBean.class.getName();
  }

  @Override
  public void postProcess(final BeanDefinitionBuilder builder, final XmlRepositoryConfigurationSource config) {
    Element element = config.getElement();
    ParsingUtils.setPropertyReference(builder, element, COUCHBASE_TEMPLATE_REF, "couchbaseOperations");
    ParsingUtils.setPropertyReference(builder, element, COUCHBASE_INDEX_MANAGER_REF, "indexManager");
  }

  @Override
  public void postProcess(final BeanDefinitionBuilder builder, final AnnotationRepositoryConfigurationSource config) {
    builder.addDependsOn(BeanNames.COUCHBASE_OPERATIONS_MAPPING);
    builder.addDependsOn(BeanNames.COUCHBASE_INDEX_MANAGER);
    builder.addPropertyReference("couchbaseOperationsMapping", BeanNames.COUCHBASE_OPERATIONS_MAPPING);
    builder.addPropertyReference("indexManager", BeanNames.COUCHBASE_INDEX_MANAGER);
  }

  /*
   * (non-Javadoc)
   * @see org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#getIdentifyingAnnotations()
   */
  @Override
  protected Collection<Class<? extends Annotation>> getIdentifyingAnnotations() {
    return Collections.singleton(Document.class);
  }

  /*
   * (non-Javadoc)
   * @see org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#getIdentifyingTypes()
   */
  @Override
  protected Collection<Class<?>> getIdentifyingTypes() {
    return Collections.singleton(CouchbaseRepository.class);
  }

  protected boolean useRepositoryConfiguration(RepositoryMetadata metadata) {
    return !metadata.isReactiveRepository();
  }
}
