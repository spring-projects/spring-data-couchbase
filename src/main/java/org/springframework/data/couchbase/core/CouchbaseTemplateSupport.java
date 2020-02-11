package org.springframework.data.couchbase.core;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.repository.support.MappingCouchbaseEntityInformation;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;

public class CouchbaseTemplateSupport {

  private final CouchbaseConverter converter;
  private final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;
  // TODO: this should be replaced I think
  private final TranslationService translationService;

  public CouchbaseTemplateSupport(final CouchbaseConverter converter) {
    this.converter = converter;
    this.mappingContext  = converter.getMappingContext();
    this.translationService = new JacksonTranslationService();
  }

  public CouchbaseDocument encodeEntity(final Object entityToEncode) {
    final CouchbaseDocument converted = new CouchbaseDocument();
    converter.write(entityToEncode, converted);
    return converted;
  }

  public <T> T decodeEntity(String id, String source, long cas, Class<T> entityClass) {
    final CouchbaseDocument converted = new CouchbaseDocument(id);
    converted.setId(id);

    T readEntity = converter.read(entityClass, (CouchbaseDocument) translationService.decode(source, converted));
    final ConvertingPropertyAccessor<T> accessor = getPropertyAccessor(readEntity);
    CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(readEntity.getClass());

    if (persistentEntity.getVersionProperty() != null) {
      accessor.setProperty(persistentEntity.getVersionProperty(), cas);
    }
    return accessor.getBean();
  }

  public void applyUpdatedCas(final Object entity, final long cas) {
    final ConvertingPropertyAccessor<Object> accessor = getPropertyAccessor(entity);
    final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(entity.getClass());
    final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();

    if (versionProperty != null) {
      accessor.setProperty(versionProperty, cas);
    }
  }

  public String getJavaNameForEntity(final Class<?> clazz) {
    final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(clazz);
    MappingCouchbaseEntityInformation<?, Object> info = new MappingCouchbaseEntityInformation<>(persistentEntity);
    return info.getJavaType().getName();
  }

  private <T> ConvertingPropertyAccessor<T> getPropertyAccessor(final T source) {
    CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(source.getClass());
    PersistentPropertyAccessor<T> accessor = entity.getPropertyAccessor(source);
    return new ConvertingPropertyAccessor<>(accessor, converter.getConversionService());
  }

}
