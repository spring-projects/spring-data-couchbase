/*
 * Copyright 2012-2017 the original author or authors
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

package org.springframework.data.couchbase.core.convert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.couchbase.client.java.repository.annotation.Field;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbaseList;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.mapping.model.SpELExpressionParameterValueProvider;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * A mapping converter for Couchbase.
 *
 * The converter is responsible for reading from and writing to entities and converting it into a
 * consumable database represenation.
 *
 * @author Michael Nitschinger
 * @author Oliver Gierke
 * @author Geoffrey Mina
 * @author Mark Paluch
 */
public class MappingCouchbaseConverter extends AbstractCouchbaseConverter
  implements ApplicationContextAware {

  /**
   * The default "type key", the name of the field that will hold type information.
   *
   * @see #TYPEKEY_SYNCGATEWAY_COMPATIBLE
   */
  public static final String TYPEKEY_DEFAULT = DefaultCouchbaseTypeMapper.DEFAULT_TYPE_KEY;

  /**
   * A "type key" (the name of the field that will hold type information) that is
   * compatible with Sync Gateway (which doesn't allows underscores).
   */
  public static final String TYPEKEY_SYNCGATEWAY_COMPATIBLE = "javaClass";

  /**
   * The overall application context.
   */
  protected ApplicationContext applicationContext;

  /**
   * The generic mapping context.
   */
  protected final MappingContext<? extends CouchbasePersistentEntity<?>,
    CouchbasePersistentProperty> mappingContext;

  /**
   * The Couchbase specific type mapper in use.
   */
  protected CouchbaseTypeMapper typeMapper;

  /**
   * Spring Expression Language context.
   */
  private final SpELContext spELContext;

  /**
   * Enable strict @Field checking on mapper
   */
  private boolean enableStrictFieldChecking = false;

  /**
   * Create a new {@link MappingCouchbaseConverter}.
   *
   * @param mappingContext the mapping context to use.
   */
  public MappingCouchbaseConverter(final MappingContext<? extends CouchbasePersistentEntity<?>,
      CouchbasePersistentProperty> mappingContext) {
    this(mappingContext, TYPEKEY_DEFAULT);
  }

  /**
   * Create a new {@link MappingCouchbaseConverter} that will store class name for
   * complex types in the <i>typeKey</i> attribute.
   *
   * @param mappingContext the mapping context to use.
   * @param typeKey the attribute name to use to store complex types class name.
   */
  public MappingCouchbaseConverter(final MappingContext<? extends CouchbasePersistentEntity<?>,
    CouchbasePersistentProperty> mappingContext, final String typeKey) {
    super(new DefaultConversionService());

    this.mappingContext = mappingContext;
    typeMapper = new DefaultCouchbaseTypeMapper(typeKey != null ? typeKey : TYPEKEY_DEFAULT);
    spELContext = new SpELContext(CouchbaseDocumentPropertyAccessor.INSTANCE);
  }

  @Override
  public MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> getMappingContext() {
    return mappingContext;
  }

  @Override
  public String getTypeKey() {
    return typeMapper.getTypeKey();
  }

  /**
   * Toggles strict checking of the couchbase {@link Field} annotation. If enabled,
   * strict checking will prevent non-annotated properties to be serialized. This only
   * applies to the Couchbase datastore, allowing other Spring Data datastores to still
   * deal with the property.
   *
   * @param enableStrictFieldChecking true to only consider Field-annotated properties for
   * Couchbase serialization.
   * @see DATACOUCH-226
   */
  public void setEnableStrictFieldChecking(boolean enableStrictFieldChecking){
    this.enableStrictFieldChecking = enableStrictFieldChecking;
  }

  @Override
  public <R> R read(final Class<R> clazz, final CouchbaseDocument source) {
    return read(ClassTypeInformation.from(clazz), source, null);
  }

  /**
   * Read an incoming {@link CouchbaseDocument} into the target entity.
   *
   * @param type the type information of the target entity.
   * @param source the document to convert.
   * @param <R> the entity type.
   * @return the converted entity.
   */
  protected <R> R read(final TypeInformation<R> type, final CouchbaseDocument source) {
    return read(type, source, null);
  }

  /**
   * Read an incoming {@link CouchbaseDocument} into the target entity.
   *
   * @param type the type information of the target entity.
   * @param source the document to convert.
   * @param parent an optional parent object.
   * @param <R> the entity type.
   * @return the converted entity.
   */
  @SuppressWarnings("unchecked")
  protected <R> R read(final TypeInformation<R> type, final CouchbaseDocument source, final Object parent) {
    if (source == null) {
      return null;
    }

    TypeInformation<? extends R> typeToUse = typeMapper.readType(source, type);
    Class<? extends R> rawType = typeToUse.getType();

    if (conversions.hasCustomReadTarget(source.getClass(), rawType)) {
      return conversionService.convert(source, rawType);
    }

    if (typeToUse.isMap()) {
      return (R) readMap(typeToUse, source, parent);
    }

    CouchbasePersistentEntity<R> entity = (CouchbasePersistentEntity<R>) mappingContext.getRequiredPersistentEntity(typeToUse);
    return read(entity, source, parent);
  }

  /**
   * Read an incoming {@link CouchbaseDocument} into the target entity.
   *
   * @param entity the target entity.
   * @param source the document to convert.
   * @param parent an optional parent object.
   * @param <R> the entity type.
   * @return the converted entity.
   */
  protected <R> R read(final CouchbasePersistentEntity<R> entity, final CouchbaseDocument source, final Object parent) {
    final DefaultSpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(source, spELContext);
    ParameterValueProvider<CouchbasePersistentProperty> provider =
        getParameterProvider(entity, source, evaluator, parent);
    EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);

    final R instance = instantiator.createInstance(entity, provider);
    final ConvertingPropertyAccessor accessor = getPropertyAccessor(instance);

    entity.getPersistentProperties().filter(prop -> {

      if (!(prop.isIdProperty() || source.containsKey(prop.getFieldName())) || entity.isConstructorArgument(prop)) {
        return false;
      }

      return true;
    }).forEach(prop -> {

      Optional<Object> obj = prop.isIdProperty() ? Optional.ofNullable(source.getId()) : getValueInternal(prop, source, instance);
      accessor.setProperty(prop, obj);
    });

    entity.getAssociations().forEach(association -> {
      CouchbasePersistentProperty inverseProp = association.getInverse();
      Optional<Object> obj = getValueInternal(inverseProp, source, instance);
      accessor.setProperty(inverseProp, obj);

    });


    return instance;
  }

  /**
   * Loads the property value through the value provider.
   *
   * @param property the source property.
   * @param source the source document.
   * @param parent the optional parent.
   * @return the actual property value.
   */
  protected Optional<Object> getValueInternal(final CouchbasePersistentProperty property, final CouchbaseDocument source,
                                    final Object parent) {
    return new CouchbasePropertyValueProvider(source, spELContext, parent).getPropertyValue(property);
  }

  /**
   * Creates a new parameter provider.
   *
   * @param entity the persistent entity.
   * @param source the source document.
   * @param evaluator the SPEL expression evaluator.
   * @param parent the optional parent.
   * @return a new parameter value provider.
   */
  private ParameterValueProvider<CouchbasePersistentProperty> getParameterProvider(
      final CouchbasePersistentEntity<?> entity, final CouchbaseDocument source,
      final DefaultSpELExpressionEvaluator evaluator, final Object parent) {
    CouchbasePropertyValueProvider provider = new CouchbasePropertyValueProvider(source, evaluator, parent);
    PersistentEntityParameterValueProvider<CouchbasePersistentProperty> parameterProvider =
        new PersistentEntityParameterValueProvider<CouchbasePersistentProperty>(entity, provider, Optional.ofNullable(parent));

    return new ConverterAwareSpELExpressionParameterValueProvider(evaluator, conversionService, parameterProvider,
        parent);
  }

  /**
   * Recursively parses the a map from the source document.
   *
   * @param type the type information for the document.
   * @param source the source document.
   * @param parent the optional parent.
   * @return the recursively parsed map.
   */
  @SuppressWarnings("unchecked")
  protected Map<Object, Object> readMap(final TypeInformation<?> type, final CouchbaseDocument source,
                                        final Object parent) {
    Assert.notNull(source, "CouchbaseDocument must not be null!");

    Class<?> mapType = typeMapper.readType(source, type).getType();
    Map<Object, Object> map = CollectionFactory.createMap(mapType, source.export().keySet().size());
    Map<String, Object> sourceMap = source.getPayload();

    for (Map.Entry<String, Object> entry : sourceMap.entrySet()) {
      Object value = entry.getValue();

      Object key = type.getComponentType().map(TypeInformation::getType) //
              .map(keyType -> (Object) conversionService.convert(entry.getKey(), keyType)) //
              .orElse(entry.getKey());

      TypeInformation<?> valueType = type.getMapValueType().orElse(null);
      if (value instanceof CouchbaseDocument) {
        map.put(key, read(valueType, (CouchbaseDocument) value, parent));
      } else if (value instanceof CouchbaseList) {
        map.put(key, readCollection(valueType, (CouchbaseList) value, parent));
      } else {
        Class<?> valueClass = valueType == null ? null : valueType.getType();
        map.put(key, getPotentiallyConvertedSimpleRead(value, valueClass));
      }
    }

    return map;
  }

  /**
   * Potentially convert simple values like ENUMs.
   *
   * @param value the value to convert.
   * @param target the target object.
   * @return the potentially converted object.
   */
  @SuppressWarnings("unchecked")
  private Object getPotentiallyConvertedSimpleRead(final Object value, final Class<?> target) {
    if (value == null || target == null) {
      return value;
    }

    if (conversions.hasCustomReadTarget(value.getClass(), target)) {
      return conversionService.convert(value, target);
    }

    if (Enum.class.isAssignableFrom(target)) {
      return Enum.valueOf((Class<Enum>) target, value.toString());
    }

    if (Class.class.isAssignableFrom(target)) {
      try {
        return Class.forName(value.toString());
      } catch (ClassNotFoundException e) {
        throw new MappingException("Unable to create class from " + value.toString());
      }
    }

    return target.isAssignableFrom(value.getClass()) ? value : conversionService.convert(value, target);
  }

  @Override
  public void write(final Object source, final CouchbaseDocument target) {
    if (source == null) {
      return;
    }

    boolean isCustom = conversions.getCustomWriteTarget(source.getClass(), CouchbaseDocument.class) != null;
    TypeInformation<?> type = ClassTypeInformation.from(source.getClass());

    if (!isCustom) {
      typeMapper.writeType(type, target);
    }

    writeInternal(source, target, type);
    if (target.getId() == null) {
      throw new MappingException("An ID property is needed, but not found on this entity.");
    }
  }

  /**
   * Convert a source object into a {@link CouchbaseDocument} target.
   *
   * @param source the source object.
   * @param target the target document.
   * @param typeHint the type information for the source.
   */
  @SuppressWarnings("unchecked")
  protected void writeInternal(final Object source, CouchbaseDocument target, final TypeInformation<?> typeHint) {
    if (source == null) {
      return;
    }

    Class<?> customTarget = conversions.getCustomWriteTarget(source.getClass(), CouchbaseDocument.class);
    if (customTarget != null) {
      copyCouchbaseDocument(conversionService.convert(source, CouchbaseDocument.class), target);
      return;
    }

    if (Map.class.isAssignableFrom(source.getClass())) {
      writeMapInternal((Map<Object, Object>) source, target, ClassTypeInformation.MAP);
      return;
    }

    if (Collection.class.isAssignableFrom(source.getClass())) {
      throw new IllegalArgumentException("Root Document must be either CouchbaseDocument or Map.");
    }

    CouchbasePersistentEntity<?> entity = mappingContext.getPersistentEntity(source.getClass()).orElse(null);
    writeInternal(source, target, entity);
    addCustomTypeKeyIfNecessary(typeHint, source, target);
  }

  /**
   * Helper method to copy the internals from a source document into a target document.
   *
   * @param source the source document.
   * @param target the target document.
   */
  protected void copyCouchbaseDocument(final CouchbaseDocument source, final CouchbaseDocument target) {
    for (Map.Entry<String, Object> entry : source.export().entrySet()) {
      target.put(entry.getKey(), entry.getValue());
    }
    target.setId(source.getId());
    target.setExpiration(source.getExpiration());
  }

  /**
   * Internal helper method to write the source object into the target document.
   *
   * @param source the source object.
   * @param target the target document.
   * @param entity the persistent entity to convert from.
   */
  protected void writeInternal(final Object source, final CouchbaseDocument target,
                               final CouchbasePersistentEntity<?> entity) {
    if (source == null) {
      return;
    }

    if (entity == null) {
      throw new MappingException("No mapping metadata found for entity of type " + source.getClass().getName());
    }

    final ConvertingPropertyAccessor accessor = getPropertyAccessor(source);
    final Optional<CouchbasePersistentProperty> idProperty = entity.getIdProperty();
    final Optional<CouchbasePersistentProperty> versionProperty = entity.getVersionProperty();

    if (target.getId() == null) {
      idProperty.ifPresent(id -> {
        target.setId(accessor.getProperty(id, String.class).orElse(null));
      });
    }
    target.setExpiration(entity.getExpiry());

    entity.getPersistentProperties().filter(prop -> {

      if (idProperty.filter(prop::equals).isPresent()) {
        return false;
      }

      if (versionProperty.filter(prop::equals).isPresent()) {
        return false;
      }

      if (enableStrictFieldChecking && !prop.isAnnotationPresent(Field.class)) {
        return false;
      }
      return true;
    }).forEach(prop -> {
      Optional<Object> propertyObj = accessor.getProperty(prop, (Class) prop.getType());
      propertyObj.ifPresent(o -> {
        if (!conversions.isSimpleType(o.getClass())) {
          writePropertyInternal(o, target, prop);
        } else {
          writeSimpleInternal(o, target, prop.getFieldName());
        }
      });
    });


    entity.getAssociations().forEach(association -> {
      CouchbasePersistentProperty inverseProp = association.getInverse();
      Class<?> type = inverseProp.getType();
      Optional<Object> propertyObj = accessor.getProperty(inverseProp, (Class) type);
      propertyObj.ifPresent(o -> {
        writePropertyInternal(o, target, inverseProp);
      });
    });

  }

  /**
   * Helper method to write a property into the target document.
   *
   * @param source the source object.
   * @param target the target document.
   * @param prop the property information.
   */
  @SuppressWarnings("unchecked")
  private void writePropertyInternal(final Object source, final CouchbaseDocument target,
                                     final CouchbasePersistentProperty prop) {
    if (source == null) {
      return;
    }

    String name = prop.getFieldName();
    TypeInformation<?> valueType = ClassTypeInformation.from(source.getClass());
    TypeInformation<?> type = prop.getTypeInformation();

    if (valueType.isCollectionLike()) {
      CouchbaseList collectionDoc = createCollection(asCollection(source), prop);
      target.put(name, collectionDoc);
      return;
    }

    if (valueType.isMap()) {
      CouchbaseDocument mapDoc = createMap((Map<Object, Object>) source, prop);
      target.put(name, mapDoc);
      return;
    }

    Class<?> basicTargetType = conversions.getCustomWriteTarget(source.getClass(), null);
    if (basicTargetType != null) {
      target.put(name, conversionService.convert(source, basicTargetType));
      return;
    }

    CouchbaseDocument propertyDoc = new CouchbaseDocument();
    addCustomTypeKeyIfNecessary(type, source, propertyDoc);

    CouchbasePersistentEntity<?> entity = isSubtype(prop.getType(), source.getClass()) ? mappingContext
        .getRequiredPersistentEntity(source.getClass()) : mappingContext.getRequiredPersistentEntity(type);
    writeInternal(source, propertyDoc, entity);
    target.put(name, propertyDoc);
  }

  /**
   * Wrapper method to create the underlying map.
   *
   * @param map the source map.
   * @param prop the persistent property.
   * @return the written couchbase document.
   */
  private CouchbaseDocument createMap(final Map<Object, Object> map, final CouchbasePersistentProperty prop) {
    Assert.notNull(map, "Given map must not be null!");
    Assert.notNull(prop, "PersistentProperty must not be null!");

    return writeMapInternal(map, new CouchbaseDocument(), prop.getTypeInformation());
  }

  /**
   * Helper method to write the map into the couchbase document.
   *
   * @param source the source object.
   * @param target the target document.
   * @param type the type information for the document.
   * @return the written couchbase document.
   */
  private CouchbaseDocument writeMapInternal(final Map<Object, Object> source, final CouchbaseDocument target,
                                             final TypeInformation<?> type) {
    for (Map.Entry<Object, Object> entry : source.entrySet()) {
      Object key = entry.getKey();
      Object val = entry.getValue();

      if (conversions.isSimpleType(key.getClass())) {
        String simpleKey = key.toString();

        if (val == null || conversions.isSimpleType(val.getClass())) {
          writeSimpleInternal(val, target, simpleKey);
        } else if (val instanceof Collection || val.getClass().isArray()) {
          target.put(simpleKey, writeCollectionInternal(asCollection(val), new CouchbaseList(conversions.getSimpleTypeHolder()), type.getMapValueType()));
        } else {
          CouchbaseDocument embeddedDoc = new CouchbaseDocument();
          TypeInformation<?> valueTypeInfo = type.isMap() ? type.getMapValueType().orElse(ClassTypeInformation.OBJECT) : ClassTypeInformation.OBJECT;
          writeInternal(val, embeddedDoc, valueTypeInfo);
          target.put(simpleKey, embeddedDoc);
        }
      } else {
        throw new MappingException("Cannot use a complex object as a key value.");
      }
    }

    return target;
  }

  /**
   * Helper method to create the underlying collection/list.
   *
   * @param collection the collection to write.
   * @param prop the property information.
   * @return the created couchbase list.
   */
  private CouchbaseList createCollection(final Collection<?> collection, final CouchbasePersistentProperty prop) {
    return writeCollectionInternal(collection, new CouchbaseList(conversions.getSimpleTypeHolder()), Optional.of(prop.getTypeInformation()));
  }

  /**
   * Helper method to write the internal collection.
   *
   * @param source the source object.
   * @param target the target document.
   * @param type the type information for the document.
   * @return the created couchbase list.
   */
  private CouchbaseList writeCollectionInternal(final Collection<?> source, final CouchbaseList target,
                                                final Optional<TypeInformation<?>> type) {

    Optional<TypeInformation<?>> componentType = type.flatMap(TypeInformation::getComponentType);

    for (Object element : source) {
      Class<?> elementType = element == null ? null : element.getClass();

      if (elementType == null || conversions.isSimpleType(elementType)) {
        target.put(element);
      } else if (element instanceof Collection || elementType.isArray()) {
        target.put(writeCollectionInternal(asCollection(element), new CouchbaseList(conversions.getSimpleTypeHolder()), componentType));
      } else {

        TypeInformation<?> typeInformation = componentType.orElse(null);
        CouchbaseDocument embeddedDoc = new CouchbaseDocument();
        writeInternal(element, embeddedDoc, typeInformation);
        target.put(embeddedDoc);
      }

    }

    return target;
  }

  /**
   * Read a collection from the source object.
   *
   * @param targetType the target type.
   * @param source the list as source.
   * @param parent the optional parent.
   * @return the instantiated collection.
   */
  @SuppressWarnings("unchecked")
  private Object readCollection(final TypeInformation<?> targetType, final CouchbaseList source, final Object parent) {
    Assert.notNull(targetType, "Target type must not be null!");

    Class<?> collectionType = targetType.getType();
    if (source.isEmpty()) {
      return getPotentiallyConvertedSimpleRead(new HashSet<Object>(), collectionType);
    }

    collectionType = Collection.class.isAssignableFrom(collectionType) ? collectionType : List.class;
    Collection<Object> items = targetType.getType().isArray() ? new ArrayList<Object>() : CollectionFactory
        .createCollection(collectionType, source.size(false));
    TypeInformation<?> componentType = targetType.getComponentType().orElse(null);
    Class<?> rawComponentType = componentType == null ? null : componentType.getType();

    for (int i = 0; i < source.size(false); i++) {

      Object dbObjItem = source.get(i);

      if (dbObjItem instanceof CouchbaseDocument) {
        items.add(read(componentType, (CouchbaseDocument) dbObjItem, parent));
      } else if (dbObjItem instanceof CouchbaseList) {
        items.add(readCollection(componentType, (CouchbaseList) dbObjItem, parent));
      } else {
        items.add(getPotentiallyConvertedSimpleRead(dbObjItem, rawComponentType));
      }
    }

    return getPotentiallyConvertedSimpleRead(items, targetType.getType());
  }

  /**
   * Returns a collection from the given source object.
   *
   * @param source the source object.
   * @return the target collection.
   */
  private static Collection<?> asCollection(final Object source) {
    if (source instanceof Collection) {
      return (Collection<?>) source;
    }

    return source.getClass().isArray() ? CollectionUtils.arrayToList(source) : Collections.singleton(source);
  }

  /**
   * Check if one class is a subtype of the other.
   *
   * @param left the first class.
   * @param right the second class.
   * @return true if it is a subtype, false otherwise.
   */
  private static boolean isSubtype(final Class<?> left, final Class<?> right) {
    return left.isAssignableFrom(right) && !left.equals(right);
  }

  /**
   * Write the given source into the couchbase document target.
   *
   * @param source the source object.
   * @param target the target document.
   * @param key the key of the object.
   */
  private void writeSimpleInternal(final Object source, final CouchbaseDocument target, final String key) {
    target.put(key, getPotentiallyConvertedSimpleWrite(source));
  }

  private Object getPotentiallyConvertedSimpleWrite(final Object value) {
    if (value == null) {
      return null;
    }

    Class<?> customTarget = conversions.getCustomWriteTarget(value.getClass(), null);
    if (customTarget != null) {
      return conversionService.convert(value, customTarget);
    } else {
      return Enum.class.isAssignableFrom(value.getClass()) ? ((Enum<?>) value).name() : value;
    }
  }

  /**
   * Add a custom type key if needed.
   *
   * @param type the type information.
   * @param source th the source object.
   * @param target the target document.
   */
  protected void addCustomTypeKeyIfNecessary(TypeInformation<?> type, Object source, CouchbaseDocument target) {
    TypeInformation<?> actualType = type != null ? type.getActualType() : type;
    Class<?> reference = actualType == null ? Object.class : actualType.getType();

    boolean notTheSameClass = !source.getClass().equals(reference);
    if (notTheSameClass) {
      typeMapper.writeType(source.getClass(), target);
    }
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }

  /**
   * Helper method to read the value based on the value type.
   *
   * @param value the value to convert.
   * @param type the type information.
   * @param parent the optional parent.
   * @param <R> the target type.
   * @return the converted object.
   */
  @SuppressWarnings("unchecked")
  private <R> R readValue(Object value, TypeInformation<?> type, Object parent) {
    Class<?> rawType = type.getType();

    if (conversions.hasCustomReadTarget(value.getClass(), rawType)) {
      return (R) conversionService.convert(value, rawType);
    } else if (value instanceof CouchbaseDocument) {
      return (R) read(type, (CouchbaseDocument) value, parent);
    } else if (value instanceof CouchbaseList) {
      return (R) readCollection(type, (CouchbaseList) value, parent);
    } else {
      return (R) getPotentiallyConvertedSimpleRead(value, rawType);
    }
  }

  private ConvertingPropertyAccessor getPropertyAccessor(Object source) {
    CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(source.getClass());
    PersistentPropertyAccessor accessor = entity.getPropertyAccessor(source);

    return new ConvertingPropertyAccessor(accessor, conversionService);
  }

  /**
   * A property value provider for Couchbase documents.
   */
  private class CouchbasePropertyValueProvider implements PropertyValueProvider<CouchbasePersistentProperty> {

    /**
     * The source document.
     */
    private final CouchbaseDocument source;

    /**
     * The expression evaluator.
     */
    private final SpELExpressionEvaluator evaluator;

    /**
     * The optional parent object.
     */
    private final Object parent;

    public CouchbasePropertyValueProvider(final CouchbaseDocument source, final SpELContext factory,
                                          final Object parent) {
      this(source, new DefaultSpELExpressionEvaluator(source, factory), parent);
    }

    public CouchbasePropertyValueProvider(final CouchbaseDocument source,
                                          final DefaultSpELExpressionEvaluator evaluator, final Object parent) {
      Assert.notNull(source, "CouchbaseDocument must not be null!");
      Assert.notNull(evaluator, "DefaultSpELExpressionEvaluator must not be null!");

      this.source = source;
      this.evaluator = evaluator;
      this.parent = parent;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> Optional<R> getPropertyValue(final CouchbasePersistentProperty property) {

      Object value = property.getSpelExpression().map(evaluator::evaluate).orElseGet(() -> source.get(property.getFieldName()));

      if (property.isIdProperty()) {
        return Optional.ofNullable((R) source.getId());
      }
      if (value == null) {
        return Optional.empty();
      }

      return Optional.ofNullable(readValue(value, property.getTypeInformation(), parent));
    }
  }

  /**
   * A expression parameter value provider.
   */
  private class ConverterAwareSpELExpressionParameterValueProvider extends
      SpELExpressionParameterValueProvider<CouchbasePersistentProperty> {

    private final Object parent;

    public ConverterAwareSpELExpressionParameterValueProvider(final SpELExpressionEvaluator evaluator,
      final ConversionService conversionService, final ParameterValueProvider<CouchbasePersistentProperty> delegate,
      final Object parent) {
      super(evaluator, conversionService, delegate);
      this.parent = parent;
    }

    @Override
    protected <T> T potentiallyConvertSpelValue(final Object object,
                                                final Parameter<T, CouchbasePersistentProperty> parameter) {
      return readValue(object, parameter.getType(), parent);
    }
  }

}
