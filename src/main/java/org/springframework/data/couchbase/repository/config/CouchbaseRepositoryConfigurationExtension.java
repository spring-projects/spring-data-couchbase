package org.springframework.data.couchbase.repository.config;


import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.data.config.ParsingUtils;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactoryBean;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.XmlRepositoryConfigurationSource;
import org.w3c.dom.Element;

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
  public void postProcess(BeanDefinitionBuilder builder, XmlRepositoryConfigurationSource config) {
    Element element = config.getElement();
    ParsingUtils.setPropertyReference(builder, element, COUCHBASE_TEMPLATE_REF, "couchbaseOperations");
  }

  @Override
  public void postProcess(BeanDefinitionBuilder builder, AnnotationRepositoryConfigurationSource config) {
    AnnotationAttributes attributes = config.getAttributes();
    builder.addPropertyReference("couchbaseOperations", attributes.getString("couchbaseTemplateRef"));
  }
}
