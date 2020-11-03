/*
 * Copyright 2012-2020 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.convert;

import static org.springframework.data.couchbase.core.mapping.id.GenerationStrategy.UNIQUE;
import static org.springframework.data.couchbase.core.mapping.id.GenerationStrategy.USE_ATTRIBUTES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbaseList;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.mapping.event.AfterConvertCallback;
import org.springframework.data.couchbase.core.mapping.id.GeneratedValue;
import org.springframework.data.couchbase.core.mapping.id.IdAttribute;
import org.springframework.data.couchbase.core.mapping.id.IdPrefix;
import org.springframework.data.couchbase.core.mapping.id.IdSuffix;
import org.springframework.data.couchbase.core.query.N1qlJoin;
import org.springframework.data.mapping.Alias;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor.Parameter;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.mapping.model.SpELExpressionParameterValueProvider;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * A mapping converter for Couchbase. The converter is responsible for reading from and writing to entities and
 * converting it into a consumable database representation.
 *
 * @author Michael Nitschinger
 * @author Oliver Gierke
 * @author Geoffrey Mina
 * @author Mark Paluch
 * @author Michael Reiche
 */
public class MappingCouchbaseConverter extends AbstractCouchbaseConverter implements ApplicationContextAware {

	/**
	 * The default "type key", the name of the field that will hold type information.
	 *
	 * @see #TYPEKEY_SYNCGATEWAY_COMPATIBLE
	 */
	public static final String TYPEKEY_DEFAULT = DefaultCouchbaseTypeMapper.DEFAULT_TYPE_KEY;

	/**
	 * A "type key" (the name of the field that will hold type information) that is compatible with Sync Gateway (which
	 * doesn't allows underscores).
	 */
	public static final String TYPEKEY_SYNCGATEWAY_COMPATIBLE = "javaClass";
	/**
	 * The generic mapping context.
	 */
	protected final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;
	/**
	 * Spring Expression Language context.
	 */
	private final SpELContext spELContext;
	/**
	 * The overall application context.
	 */
	protected ApplicationContext applicationContext;
	/**
	 * The Couchbase specific type mapper in use.
	 */
	protected CouchbaseTypeMapper typeMapper;

	/**
	 * Callbacks for Audit Mechanism
	 */
	private @Nullable EntityCallbacks entityCallbacks;

	public MappingCouchbaseConverter() {
		super(new DefaultConversionService());

		this.typeMapper = new DefaultCouchbaseTypeMapper(TYPEKEY_DEFAULT);
		this.mappingContext = new CouchbaseMappingContext();
		this.spELContext = new SpELContext(CouchbaseDocumentPropertyAccessor.INSTANCE);
	}

	/**
	 * Create a new {@link MappingCouchbaseConverter}.
	 *
	 * @param mappingContext the mapping context to use.
	 */
	public MappingCouchbaseConverter(
			final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext) {
		this(mappingContext, TYPEKEY_DEFAULT);
	}

	/**
	 * Create a new {@link MappingCouchbaseConverter} that will store class name for complex types in the <i>typeKey</i>
	 * attribute.
	 *
	 * @param mappingContext the mapping context to use.
	 * @param typeKey the attribute name to use to store complex types class name.
	 */
	public MappingCouchbaseConverter(
			final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext,
			final String typeKey) {
		super(new DefaultConversionService());

		this.mappingContext = mappingContext;
		typeMapper = new DefaultCouchbaseTypeMapper(typeKey != null ? typeKey : TYPEKEY_DEFAULT);
		spELContext = new SpELContext(CouchbaseDocumentPropertyAccessor.INSTANCE);
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

	@Override
	public MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> getMappingContext() {
		return mappingContext;
	}

	@Override
	public String getTypeKey() {
		return typeMapper.getTypeKey();
	}

	@Override
	public Alias getTypeAlias(TypeInformation<?> info) {
		return typeMapper.getTypeAlias(info);
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

		CouchbasePersistentEntity<R> entity = (CouchbasePersistentEntity<R>) mappingContext
				.getRequiredPersistentEntity(typeToUse);
		return read(entity, source, parent);
	}

	private boolean isIdConstructionProperty(final CouchbasePersistentProperty property) {
		return property.isAnnotationPresent(IdPrefix.class) || property.isAnnotationPresent(IdSuffix.class);
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
		ParameterValueProvider<CouchbasePersistentProperty> provider = getParameterProvider(entity, source, evaluator,
				parent);
		EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);

		final R instance = instantiator.createInstance(entity, provider);
		final ConvertingPropertyAccessor accessor = getPropertyAccessor(instance);

		entity.doWithProperties(new PropertyHandler<CouchbasePersistentProperty>() {
			@Override
			public void doWithPersistentProperty(final CouchbasePersistentProperty prop) {
				if (!doesPropertyExistInSource(prop) || entity.isConstructorArgument(prop) || isIdConstructionProperty(prop)
						|| prop.isAnnotationPresent(N1qlJoin.class)) {
					return;
				}
				Object obj = prop.isIdProperty() && parent == null ? source.getId() : getValueInternal(prop, source, instance);
				accessor.setProperty(prop, obj);
			}

			private boolean doesPropertyExistInSource(final CouchbasePersistentProperty property) {
				return property.isIdProperty() || source.containsKey(property.getFieldName());
			}

			private boolean isIdConstructionProperty(final CouchbasePersistentProperty property) {
				return property.isAnnotationPresent(IdPrefix.class) || property.isAnnotationPresent(IdSuffix.class);
			}
		});

		entity.doWithAssociations((AssociationHandler<CouchbasePersistentProperty>) association -> {
			CouchbasePersistentProperty inverseProp = association.getInverse();
			Object obj = getValueInternal(inverseProp, source, instance);
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
	protected Object getValueInternal(final CouchbasePersistentProperty property, final CouchbaseDocument source,
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
		PersistentEntityParameterValueProvider<CouchbasePersistentProperty> parameterProvider = new PersistentEntityParameterValueProvider<>(
				entity, provider, parent);

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
		Map<String, Object> sourceMap = source.getContent();

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

		boolean isCustom = conversions.getCustomWriteTarget(source.getClass(), CouchbaseDocument.class).isPresent();
		TypeInformation<?> type = ClassTypeInformation.from(source.getClass());

		if (!isCustom) {
			typeMapper.writeType(type, target);
		}

		writeInternal(source, target, type, true);
		if (target.getId() == null) {
			throw new MappingException("An ID property is needed, but not found/could not be generated on this entity.");
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
	protected void writeInternal(final Object source, CouchbaseDocument target, final TypeInformation<?> typeHint,
			boolean withId) {
		if (source == null) {
			return;
		}

		Optional<Class<?>> customTarget = conversions.getCustomWriteTarget(source.getClass(), CouchbaseDocument.class);
		if (customTarget.isPresent()) {
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

		CouchbasePersistentEntity<?> entity = mappingContext.getPersistentEntity(source.getClass());
		writeInternal(source, target, entity, withId);
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

	private String convertToString(Object propertyObj) {
		if (propertyObj instanceof String) {
			return (String) propertyObj;
		} else if (propertyObj instanceof Number) {
			return new StringBuffer().append(propertyObj).toString();
		} else {
			return propertyObj.toString();
		}
	}

	/**
	 * Internal helper method to write the source object into the target document.
	 *
	 * @param source the source object.
	 * @param target the target document.
	 * @param entity the persistent entity to convert from.
	 * @param withId one of the top-level properties is the id for the document
	 */
	protected void writeInternal(final Object source, final CouchbaseDocument target,
			final CouchbasePersistentEntity<?> entity, boolean withId) {
		if (source == null) {
			return;
		}

		if (entity == null) {
			throw new MappingException("No mapping metadata found for entity of type " + source.getClass().getName());
		}

		final ConvertingPropertyAccessor<Object> accessor = getPropertyAccessor(source);
		final CouchbasePersistentProperty idProperty = withId ? entity.getIdProperty() : null;
		final CouchbasePersistentProperty versionProperty = entity.getVersionProperty();

		GeneratedValue generatedValueInfo = null;
		final TreeMap<Integer, String> prefixes = new TreeMap<>();
		final TreeMap<Integer, String> suffixes = new TreeMap<>();
		final TreeMap<Integer, String> idAttributes = new TreeMap<>();

		target.setExpiration(entity.getExpiry());

		entity.doWithProperties(new PropertyHandler<CouchbasePersistentProperty>() {
			@Override
			public void doWithPersistentProperty(final CouchbasePersistentProperty prop) {
				if (prop.equals(idProperty) || (versionProperty != null && prop.equals(versionProperty))) {
					return;
				} else if (prop.isAnnotationPresent(N1qlJoin.class)) {
					return;
				}

				Object propertyObj = accessor.getProperty(prop, prop.getType());
				if (null != propertyObj) {
					if (prop.isAnnotationPresent(IdPrefix.class)) {
						IdPrefix prefix = prop.findAnnotation(IdPrefix.class);
						int order = prefix.order();
						prefixes.put(order, convertToString(propertyObj));
						return;
					}

					if (prop.isAnnotationPresent(IdSuffix.class)) {
						IdSuffix suffix = prop.findAnnotation(IdSuffix.class);
						int order = suffix.order();
						suffixes.put(order, convertToString(propertyObj));
						return;
					}

					if (prop.isAnnotationPresent(IdAttribute.class)) {
						IdAttribute idAttribute = prop.findAnnotation(IdAttribute.class);
						int order = idAttribute.order();
						idAttributes.put(order, convertToString(propertyObj));
					}

					if (!conversions.isSimpleType(propertyObj.getClass())) {
						writePropertyInternal(propertyObj, target, prop, false);
					} else {
						writeSimpleInternal(propertyObj, target, prop.getFieldName());
					}
				}
			}
		});

		if (idProperty != null && target.getId() == null) {
			String id = accessor.getProperty(idProperty, String.class);
			if (idProperty.isAnnotationPresent(GeneratedValue.class) && (id == null || id.equals(""))) {
				generatedValueInfo = idProperty.findAnnotation(GeneratedValue.class);
				String generatedId = generateId(generatedValueInfo, prefixes, suffixes, idAttributes);
				target.setId(generatedId);
				// this is not effective if id is Immutable, and accessor.setProperty() returns a new object in getBean()
				accessor.setProperty(idProperty, generatedId);
			} else {
				target.setId(id);
			}
		}

		entity.doWithAssociations(new AssociationHandler<CouchbasePersistentProperty>() {
			@Override
			public void doWithAssociation(final Association<CouchbasePersistentProperty> association) {
				CouchbasePersistentProperty inverseProp = association.getInverse();
				Class<?> type = inverseProp.getType();
				Object propertyObj = accessor.getProperty(inverseProp, type);
				if (null != propertyObj) {
					writePropertyInternal(propertyObj, target, inverseProp, false);
				}
			}
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
			final CouchbasePersistentProperty prop, boolean withId) {
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

		if (valueType.getType().equals(java.util.Optional.class)) {
			if (source == null)
				return;
			Optional<String> o = (Optional<String>) source;
			if (o.isPresent()) {
				writeSimpleInternal(o.get(), target, prop.getFieldName());
			} else {
				writeSimpleInternal(null, target, prop.getFieldName());
			}
			return;
		}

		Optional<Class<?>> basicTargetType = conversions.getCustomWriteTarget(source.getClass());
		if (basicTargetType.isPresent()) {

			basicTargetType.ifPresent(it -> {
				target.put(name, conversionService.convert(source, it));
			});

			return;
		}

		CouchbaseDocument propertyDoc = new CouchbaseDocument();
		addCustomTypeKeyIfNecessary(type, source, propertyDoc);

		CouchbasePersistentEntity<?> entity = isSubtype(prop.getType(), source.getClass())
				? mappingContext.getRequiredPersistentEntity(source.getClass())
				: mappingContext.getRequiredPersistentEntity(type);
		writeInternal(source, propertyDoc, entity, false);
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
					target.put(simpleKey, writeCollectionInternal(asCollection(val),
							new CouchbaseList(conversions.getSimpleTypeHolder()), type.getMapValueType()));
				} else {
					CouchbaseDocument embeddedDoc = new CouchbaseDocument();
					TypeInformation<?> valueTypeInfo = type.isMap() ? type.getMapValueType() : ClassTypeInformation.OBJECT;
					writeInternal(val, embeddedDoc, valueTypeInfo, false);
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
		return writeCollectionInternal(collection, new CouchbaseList(conversions.getSimpleTypeHolder()),
				prop.getTypeInformation());
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
			final TypeInformation<?> type) {
		TypeInformation<?> componentType = type == null ? null : type.getComponentType();

		for (Object element : source) {
			Class<?> elementType = element == null ? null : element.getClass();

			if (elementType == null || conversions.isSimpleType(elementType)) {
				target.put(getPotentiallyConvertedSimpleWrite(element));
			} else if (element instanceof Collection || elementType.isArray()) {
				target.put(writeCollectionInternal(asCollection(element), new CouchbaseList(conversions.getSimpleTypeHolder()),
						componentType));
			} else {

				CouchbaseDocument embeddedDoc = new CouchbaseDocument();
				writeInternal(element, embeddedDoc, componentType, false);
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
		Collection<Object> items = targetType.getType().isArray() ? new ArrayList<Object>()
				: CollectionFactory.createCollection(collectionType, source.size(false));
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

		Optional<Class<?>> customTarget = conversions.getCustomWriteTarget(value.getClass());

		return customTarget.map(it -> (Object) conversionService.convert(value, it))
				.orElseGet(() -> Enum.class.isAssignableFrom(value.getClass()) ? ((Enum<?>) value).name() : value);
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
		if (entityCallbacks == null) {
			setEntityCallbacks(EntityCallbacks.create(applicationContext));
		}
	}

	/**
	 * COPIED Set the {@link EntityCallbacks} instance to use when invoking
	 * {@link org.springframework.data.mapping.callback.EntityCallback callbacks} like the {@link AfterConvertCallback}.
	 * <p/>
	 * Overrides potentially existing {@link EntityCallbacks}.
	 *
	 * @param entityCallbacks must not be {@literal null}.
	 * @throws IllegalArgumentException if the given instance is {@literal null}.
	 * @since 3.0
	 */
	public void setEntityCallbacks(EntityCallbacks entityCallbacks) {
		Assert.notNull(entityCallbacks, "EntityCallbacks must not be null!");
		this.entityCallbacks = entityCallbacks;
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

	private ConvertingPropertyAccessor<Object> getPropertyAccessor(Object source) {

		CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(source.getClass());
		PersistentPropertyAccessor<Object> accessor = entity.getPropertyAccessor(source);

		return new ConvertingPropertyAccessor<>(accessor, conversionService);
	}

	private String generateId(GeneratedValue generatedValue, TreeMap<Integer, String> prefixes,
			TreeMap<Integer, String> suffixes, TreeMap<Integer, String> idAttributes) {
		String delimiter = generatedValue.delimiter();
		StringBuilder sb = new StringBuilder();
		boolean isAppending = false;
		if (prefixes.size() > 0) {
			appendKeyParts(sb, prefixes.values(), delimiter);
			isAppending = true;
		}

		if (generatedValue.strategy() == USE_ATTRIBUTES && idAttributes.size() > 0) {
			if (isAppending) {
				sb.append(delimiter);
			}
			appendKeyParts(sb, idAttributes.values(), delimiter);
			isAppending = true;
		}

		if (generatedValue.strategy() == UNIQUE) {
			if (isAppending) {
				sb.append(delimiter);
			}
			sb.append(UUID.randomUUID());
			isAppending = true;
		}

		if (suffixes.size() > 0) {
			if (isAppending) {
				sb.append(delimiter);
			}
			appendKeyParts(sb, suffixes.values(), delimiter);
		}
		return sb.toString();
	}

	private StringBuilder appendKeyParts(StringBuilder sb, Collection<String> values, String delimiter) {
		boolean isAppending = false;
		for (String value : values) {
			if (isAppending) {
				sb.append(delimiter);
			} else {
				isAppending = true;
			}
			sb.append(value);
		}
		return sb;
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
		public <R> R getPropertyValue(final CouchbasePersistentProperty property) {
			String expression = property.getSpelExpression();
			Object value = expression != null ? evaluator.evaluate(expression) : source.get(property.getFieldName());

			if (property.isIdProperty() && parent == null) {
				return (R) source.getId();
			}
			if (value == null) {
				return null;
			}

			return readValue(value, property.getTypeInformation(), parent);
		}
	}

	/**
	 * A expression parameter value provider.
	 */
	private class ConverterAwareSpELExpressionParameterValueProvider
			extends SpELExpressionParameterValueProvider<CouchbasePersistentProperty> {

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
