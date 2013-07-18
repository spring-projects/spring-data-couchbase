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

package org.springframework.data.couchbase.core.convert;

import java.util.*;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.couchbase.core.mapping.*;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.*;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * The Couchbase special {@link MappingCouchbaseConverter}.
 *
 * This converter is responsible for mapping (read and writing) value from and to target formats.
 *
 * @author Michael Nitschinger
 */
public class MappingCouchbaseConverter extends AbstractCouchbaseConverter
  implements ApplicationContextAware {

  protected ApplicationContext applicationContext;
  protected final MappingContext<? extends CouchbasePersistentEntity<?>,
      CouchbasePersistentProperty> mappingContext;
  protected boolean useFieldAccessOnly = true;
  protected CouchbaseTypeMapper typeMapper;
  private SpELContext spELContext;

  @SuppressWarnings("deprecation")
  public MappingCouchbaseConverter(MappingContext<? extends CouchbasePersistentEntity<?>,
      CouchbasePersistentProperty> mappingContext) {
    super(ConversionServiceFactory.createDefaultConversionService());

    this.mappingContext = mappingContext;
    typeMapper = new DefaultCouchbaseTypeMapper(DefaultCouchbaseTypeMapper.DEFAULT_TYPE_KEY);

    spELContext = new SpELContext(CouchbaseDocumentPropertyAccessor.INSTANCE);
  }

  @Override
  public MappingContext<? extends CouchbasePersistentEntity<?>,
    CouchbasePersistentProperty> getMappingContext() {
    return mappingContext;
  }

  @Override
  public <R> R read(Class<R> clazz, CouchbaseDocument doc) {
  	return read(ClassTypeInformation.from(clazz), doc, null);
  }

  protected <R> R read(TypeInformation<R> type, CouchbaseDocument doc) {
    return read(type, doc, null);
  }
  
  protected <R> R read(TypeInformation<R> type, final CouchbaseDocument source, Object parent) {

    if (source == null) {
      return null;
    }

    TypeInformation<? extends R> typeToUse = typeMapper.readType(source, type);
    Class<? extends R> rawType = typeToUse.getType();

    if (typeToUse.isMap()) {
      return (R) readMap(typeToUse, source, parent);
    }

    CouchbasePersistentEntity<R> persistentEntity = (CouchbasePersistentEntity<R>)
      mappingContext.getPersistentEntity(typeToUse);

    if (persistentEntity == null) {
      throw new MappingException("No mapping metadata found for " + rawType.getName());
    }

    return read(persistentEntity, source, parent);
  }

  protected <R extends Object> R read(final CouchbasePersistentEntity<R> entity, final CouchbaseDocument source, final Object parent) {
    final DefaultSpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(source, spELContext);

    ParameterValueProvider<CouchbasePersistentProperty> provider = getParameterProvider(entity, source, evaluator, parent);
    EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);
    R instance = instantiator.createInstance(entity, provider);

    final BeanWrapper<CouchbasePersistentEntity<R>, R> wrapper = BeanWrapper.create(instance, conversionService);
    final R result = wrapper.getBean();

    entity.doWithProperties(new PropertyHandler<CouchbasePersistentProperty>() {
      public void doWithPersistentProperty(final CouchbasePersistentProperty prop) {
        if (!source.containsKey(prop.getFieldName()) || entity.isConstructorArgument(prop)) {
          return;
        }

        Object obj = prop.isIdProperty() ? source.getId() : getValueInternal(prop, source, evaluator, result);
        wrapper.setProperty(prop, obj, useFieldAccessOnly);
      }
    });

    entity.doWithAssociations(new AssociationHandler<CouchbasePersistentProperty>() {
      public void doWithAssociation(final Association<CouchbasePersistentProperty> association) {
        CouchbasePersistentProperty inverseProp = association.getInverse();
        Object obj = getValueInternal(inverseProp, source, evaluator, result);

        wrapper.setProperty(inverseProp, obj);
      }
    });

    return result;
  }

  protected Object getValueInternal(CouchbasePersistentProperty prop, CouchbaseDocument source, SpELExpressionEvaluator eval,
                                    Object parent) {

    CouchbasePropertyValueProvider provider = new CouchbasePropertyValueProvider(source, spELContext, parent);
    return provider.getPropertyValue(prop);
  }

  private ParameterValueProvider<CouchbasePersistentProperty> getParameterProvider(CouchbasePersistentEntity<?> entity,
    CouchbaseDocument source, DefaultSpELExpressionEvaluator evaluator, Object parent) {

    CouchbasePropertyValueProvider provider = new CouchbasePropertyValueProvider(source, evaluator, parent);
    PersistentEntityParameterValueProvider<CouchbasePersistentProperty> parameterProvider = new PersistentEntityParameterValueProvider<CouchbasePersistentProperty>(
      entity, provider, parent);
    return new ConverterAwareSpELExpressionParameterValueProvider(evaluator, conversionService, parameterProvider,
      parent);
  }

  protected Map<Object, Object> readMap(TypeInformation<?> type, CouchbaseDocument doc, Object parent) {
    Assert.notNull(doc);

    Class<?> mapType = typeMapper.readType(doc, type).getType();
    Map<Object, Object> map = CollectionFactory.createMap(mapType, doc.export().keySet().size());
    Map<String, Object> sourceMap = doc.getPayload();

    for (Map.Entry<String, Object> entry : sourceMap.entrySet()) {
      Object key = entry.getKey();
      Object value = entry.getValue();

      TypeInformation<?> keyTypeInformation = type.getComponentType();
      if (keyTypeInformation != null) {
        Class<?> keyType = keyTypeInformation.getType();
        key = conversionService.convert(key, keyType);
      }

      TypeInformation<?> valueType = type.getMapValueType();

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

  private Object getPotentiallyConvertedSimpleRead(Object value, Class<?> target) {

    if (value == null || target == null) {
      return value;
    }

    if (Enum.class.isAssignableFrom(target)) {
      return Enum.valueOf((Class<Enum>) target, value.toString());
    }

    return target.isAssignableFrom(value.getClass()) ? value : conversionService.convert(value, target);
  }


  @Override
  public void write(final Object source, final CouchbaseDocument target) {
    if (source == null) {
      return;
    }

    TypeInformation<? extends Object> type = ClassTypeInformation.from(source.getClass());
    typeMapper.writeType(type, target);
    writeInternal(source, target, type);

    if (target.getId() == null) {
      throw new MappingException("An ID property is needed, but not found on this entity.");
    }
  }

  protected void writeInternal(final Object source, final CouchbaseDocument target, final TypeInformation<?> typeHint) {
    if (source == null) {
      return;
    }

    if (Map.class.isAssignableFrom(source.getClass())) {
      writeMapInternal((Map<Object, Object>) source, target, ClassTypeInformation.MAP);
      return;
    }

    if (Collection.class.isAssignableFrom(source.getClass())) {
      throw new IllegalArgumentException("Root Document must be either CouchbaseDocument or Map.");
    }

    CouchbasePersistentEntity<?> entity = mappingContext.getPersistentEntity(source.getClass());
    writeInternal(source, target, entity);
    addCustomTypeKeyIfNecessary(typeHint, source, target);
  }

  protected void writeInternal(final Object source, final CouchbaseDocument target, final CouchbasePersistentEntity<?> entity) {
    if (source == null) {
      return;
    }

    if (entity == null) {
      throw new MappingException("No mapping metadata found for entity of type " + source.getClass().getName());
    }

    final BeanWrapper<CouchbasePersistentEntity<Object>, Object> wrapper = BeanWrapper.create(source, conversionService);
    final CouchbasePersistentProperty idProperty = entity.getIdProperty();
    if (idProperty != null && target.getId() == null) {
      String id = wrapper.getProperty(idProperty, String.class, useFieldAccessOnly);
      target.setId(id);
    }
    target.setExpiration(entity.getExpiry());

    entity.doWithProperties(new PropertyHandler<CouchbasePersistentProperty>() {
      public void doWithPersistentProperty(final CouchbasePersistentProperty prop) {
        if (prop.equals(idProperty)) {
          return;
        }

        Object propertyObj = wrapper.getProperty(prop, prop.getType(), useFieldAccessOnly);
        if (null != propertyObj) {
          if (!conversions.isSimpleType(propertyObj.getClass())) {
            writePropertyInternal(propertyObj, target, prop);
          } else {
            writeSimpleInternal(propertyObj, target, prop.getFieldName());
          }
        }
      }
    });

    entity.doWithAssociations(new AssociationHandler<CouchbasePersistentProperty>() {
      @Override
      public void doWithAssociation(final Association<CouchbasePersistentProperty> association) {
        CouchbasePersistentProperty inverseProp = association.getInverse();
        Class<?> type = inverseProp.getType();
        Object propertyObj = wrapper.getProperty(inverseProp, type, useFieldAccessOnly);
        if (null != propertyObj) {
          writePropertyInternal(propertyObj, target, inverseProp);
        }
      }
    });

  }

  private void writePropertyInternal(final Object source, final CouchbaseDocument target, final CouchbasePersistentProperty prop) {
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

    CouchbaseDocument propertyDoc = new CouchbaseDocument();
    addCustomTypeKeyIfNecessary(type, source, propertyDoc);

    CouchbasePersistentEntity<?> entity = isSubtype(prop.getType(), source.getClass()) ? mappingContext
      .getPersistentEntity(source.getClass()) : mappingContext.getPersistentEntity(type);
    writeInternal(source, propertyDoc, entity);
    target.put(name, propertyDoc);
  }

  private CouchbaseDocument createMap(Map<Object, Object> map, CouchbasePersistentProperty prop) {
    Assert.notNull(map, "Given map must not be null!");
    Assert.notNull(prop, "PersistentProperty must not be null!");

    return writeMapInternal(map, new CouchbaseDocument(), prop.getTypeInformation());
  }

  private CouchbaseDocument writeMapInternal(Map<Object,Object> source, CouchbaseDocument target, TypeInformation<?> type) {
    for (Map.Entry<Object, Object> entry : source.entrySet()) {
      Object key = entry.getKey();
      Object val = entry.getValue();

      if (conversions.isSimpleType(key.getClass())) {
        String simpleKey = key.toString();

        if (val == null || conversions.isSimpleType(val.getClass())) {
          writeSimpleInternal(val, target, simpleKey);
        } else if (val instanceof Collection || val.getClass().isArray()) {
          target.put(simpleKey, writeCollectionInternal(asCollection(val), new CouchbaseList(), type.getMapValueType()));
        } else {
          CouchbaseDocument embeddedDoc = new CouchbaseDocument();
          TypeInformation<?> valueTypeInfo = type.isMap() ? type.getMapValueType() : ClassTypeInformation.OBJECT;
          writeInternal(val, embeddedDoc, valueTypeInfo);
          target.put(simpleKey, embeddedDoc);
        }
      } else {
        throw new MappingException("Cannot use a complex object as a key value.");
      }
    }

    return target;
  }

  private CouchbaseList createCollection(Collection<?> collection, CouchbasePersistentProperty prop) {
    return writeCollectionInternal(collection, new CouchbaseList(), prop.getTypeInformation());
  }

  private CouchbaseList writeCollectionInternal(Collection<?> source, CouchbaseList target, TypeInformation<?> type) {
    TypeInformation<?> componentType = type == null ? null : type.getComponentType();

    for (Object element : source) {
      Class<?> elementType = element == null ? null : element.getClass();

      if (elementType == null || conversions.isSimpleType(elementType)) {
        target.put(element);
      } else if (element instanceof Collection || elementType.isArray()) {
        target.put(writeCollectionInternal(asCollection(element), new CouchbaseList(), componentType));
      } else {
        CouchbaseDocument embeddedDoc = new CouchbaseDocument();
        writeInternal(element, embeddedDoc, componentType);
        target.put(embeddedDoc);
      }

    }

    return target;
  }

  private Object readCollection(final TypeInformation<?> targetType, final CouchbaseList source, final Object parent) {
    Assert.notNull(targetType);

    Class<?> collectionType = targetType.getType();
    if (source.isEmpty()) {
      return getPotentiallyConvertedSimpleRead(new HashSet<Object>(), collectionType);
    }

    collectionType = Collection.class.isAssignableFrom(collectionType) ? collectionType : List.class;
    Collection<Object> items = targetType.getType().isArray() ? new ArrayList<Object>() : CollectionFactory
      .createCollection(collectionType, source.size(false));
    TypeInformation<?> componentType = targetType.getComponentType();
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


  private static Collection<?> asCollection(final Object source) {

    if (source instanceof Collection) {
      return (Collection<?>) source;
    }

    return source.getClass().isArray() ? CollectionUtils.arrayToList(source) : Collections.singleton(source);
  }

  private boolean isSubtype(final Class<?> left, final Class<?> right) {
    return left.isAssignableFrom(right) && !left.equals(right);
  }

  private void writeSimpleInternal(Object source, CouchbaseDocument target, String key) {
    target.put(key, source);
  }

  protected void addCustomTypeKeyIfNecessary(TypeInformation<?> type, Object source, CouchbaseDocument target) {
    TypeInformation<?> actualType = type != null ? type.getActualType() : type;
    Class<?> reference = actualType == null ? Object.class : actualType.getType();

    boolean notTheSameClass = !source.getClass().equals(reference);
    if (notTheSameClass) {
      typeMapper.writeType(source.getClass(), target);
    }
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext)
    throws BeansException {
    this.applicationContext = applicationContext;
  }

  private class CouchbasePropertyValueProvider implements PropertyValueProvider<CouchbasePersistentProperty> {

    private final CouchbaseDocument source;
    private final SpELExpressionEvaluator evaluator;
    private final Object parent;

    public CouchbasePropertyValueProvider(CouchbaseDocument source, SpELContext factory, Object parent) {
      this(source, new DefaultSpELExpressionEvaluator(source, factory), parent);
    }

    public CouchbasePropertyValueProvider(CouchbaseDocument source, DefaultSpELExpressionEvaluator evaluator, Object parent) {

      Assert.notNull(source);
      Assert.notNull(evaluator);

      this.source = source;
      this.evaluator = evaluator;
      this.parent = parent;
    }

    public <R> R getPropertyValue(final CouchbasePersistentProperty property) {

      String expression = property.getSpelExpression();
      Object value = expression != null ? evaluator.evaluate(expression) : source.get(property.getFieldName());

      if (property.isIdProperty()) {
        return (R) source.getId();
      }

      if (value == null) {
        return null;
      }

      return readValue(value, property.getTypeInformation(), parent);
    }
  }

  private <R> R readValue(Object value, TypeInformation<?> type, Object parent) {
    Class<?> rawType = type.getType();

    if (value instanceof CouchbaseDocument) {
      return (R) read(type, (CouchbaseDocument) value, parent);
    } else if (value instanceof CouchbaseList) {
      return (R) readCollection(type, (CouchbaseList) value, parent);
    } else {
      return (R) getPotentiallyConvertedSimpleRead(value, rawType);
    }
  }

  private class ConverterAwareSpELExpressionParameterValueProvider extends
    SpELExpressionParameterValueProvider<CouchbasePersistentProperty> {
    private final Object parent;

    public ConverterAwareSpELExpressionParameterValueProvider(SpELExpressionEvaluator evaluator,
       ConversionService conversionService, ParameterValueProvider<CouchbasePersistentProperty> delegate, Object parent) {

      super(evaluator, conversionService, delegate);
      this.parent = parent;
    }

    @Override
    protected <T> T potentiallyConvertSpelValue(Object object, Parameter<T, CouchbasePersistentProperty> parameter) {
      return readValue(object, parameter.getType(), parent);
    }
  }

}
