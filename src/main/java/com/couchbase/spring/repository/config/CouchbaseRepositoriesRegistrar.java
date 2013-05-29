package com.couchbase.spring.repository.config;

import org.springframework.data.repository.config.RepositoryBeanDefinitionRegistrarSupport;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;

import java.lang.annotation.Annotation;


public class CouchbaseRepositoriesRegistrar extends RepositoryBeanDefinitionRegistrarSupport {

  @Override
  protected Class<? extends Annotation> getAnnotation() {
    return EnableCouchbaseRepositories.class;
  }

  @Override
  protected RepositoryConfigurationExtension getExtension() {
    return new CouchbaseRepositoryConfigurationExtension();
  }

}
