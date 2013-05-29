package com.couchbase.spring.repository.support;

import com.couchbase.spring.core.mapping.CouchbasePersistentEntity;
import com.couchbase.spring.core.mapping.CouchbasePersistentProperty;
import com.couchbase.spring.repository.query.CouchbaseEntityInformation;
import org.springframework.data.mapping.model.BeanWrapper;
import org.springframework.data.repository.core.support.AbstractEntityInformation;

import java.io.Serializable;


public class MappingCouchbaseEntityInformation<T, ID  extends Serializable>
  extends AbstractEntityInformation<T, ID>
  implements CouchbaseEntityInformation<T, ID> {

  private final CouchbasePersistentEntity<T> entityMetadata;

  public MappingCouchbaseEntityInformation(CouchbasePersistentEntity<T> entity) {
    super(entity.getType());
    entityMetadata = entity;
  }

  @Override
  public ID getId(T entity) {
    CouchbasePersistentProperty idProperty = entityMetadata.getIdProperty();

    if(idProperty == null) {
      return null;
    }

    try {
      return (ID) BeanWrapper.create(entity, null).getProperty(idProperty);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Class<ID> getIdType() {
    return (Class<ID>) entityMetadata.getIdProperty().getType();
  }
}
