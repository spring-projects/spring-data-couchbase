/*
 * Copyright 2012-2025 the original author or authors
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

import static org.springframework.data.couchbase.core.mapping.id.GenerationStrategy.*;

import java.beans.Transient;
import java.lang.reflect.InaccessibleObjectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.env.Environment;
import org.springframework.core.env.EnvironmentCapable;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.PropertyValueConverter;
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
import org.springframework.data.mapping.Parameter;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.CachingValueExpressionEvaluatorFactory;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.EntityInstantiator;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.mapping.model.ValueExpressionParameterValueProvider;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.java.encryption.annotation.Encrypted;
import com.couchbase.client.java.json.JsonObject;

/**
 * A mapping converter for Couchbase. The converter is responsible for reading from and writing to entities and
 * converting it into a consumable database representation.
 *
 * @author Michael Nitschinger
 * @author Oliver Gierke
 * @author Geoffrey Mina
 * @author Mark Paluch
 * @author Michael Reiche
 * @author Remi Bleuse
 * @author Vipul Gupta
 */
public class MappingCouchbaseConverter extends AbstractCouchbaseConverter implements ApplicationContextAware, EnvironmentCapable, EnvironmentAware {

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

	private final SpelExpressionParser expressionParser = new SpelExpressionParser();

	private final CachingValueExpressionEvaluatorFactory expressionEvaluatorFactory;
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

	private @Nullable Environment environment;

	public MappingCouchbaseConverter() {
		this(new CouchbaseMappingContext(), null);
	}

	/**
	 * Create a new {@link MappingCouchbaseConverter}.
	 *
	 * @param mappingContext the mapping context to use.
	 */
	public MappingCouchbaseConverter(
			final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext) {
		this(mappingContext, null);
	}

	/**
	 * Create a new {@link MappingCouchbaseConverter}
	 *
	 * @param mappingContext the mapping context to use.
	 * @param typeKey the attribute name to use to store complex types class name.
	 */
	public MappingCouchbaseConverter(
			final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext,
			final String typeKey) {
		this(mappingContext, typeKey, new CouchbaseCustomConversions(Collections.emptyList()));
	}

	/**
	 * Create a new {@link MappingCouchbaseConverter} that will store class name for complex types in the <i>typeKey</i>
	 * attribute.
	 *
	 * @param mappingContext the mapping context to use.
	 * @param typeKey the attribute name to use to store complex types class name.
	 * @param customConversions the custom conversions to use
	 */
	public MappingCouchbaseConverter(
			final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext,
			final String typeKey,
			final CustomConversions customConversions) {
		super(new DefaultConversionService(), customConversions);
		this.mappingContext = mappingContext;
		// Don't rely on setSimpleTypeHolder being called in afterPropertiesSet() - some integration tests do not use it
		// if the mappingContext does not have the SimpleTypes, it will not know that they have converters, then it will
		// try to access the fields of the type and (maybe) fail with InaccessibleObjectException
		((CouchbaseMappingContext) mappingContext).setSimpleTypeHolder(customConversions.getSimpleTypeHolder());
		typeMapper = new DefaultCouchbaseTypeMapper(typeKey != null ? typeKey : TYPEKEY_DEFAULT);
		spELContext = new SpELContext(CouchbaseDocumentPropertyAccessor.INSTANCE);

		expressionEvaluatorFactory = new CachingValueExpressionEvaluatorFactory(
				expressionParser, this, o -> spELContext.getEvaluationContext(o));
	}

	/**
	 * Returns a collection from the given source object.
	 *
	 * @param source the source object.
	 * @return the target collection.
	 */
	protected static Collection<?> asCollection(final Object source) {
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
	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	@Override
	public Environment getEnvironment() {

		if (this.environment == null) {
			this.environment = new StandardEnvironment();
		}

		return environment;
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
		return read(TypeInformation.of(clazz), source, null);
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

		ValueExpressionEvaluator evaluator = expressionEvaluatorFactory.create(source);
		ParameterValueProvider<CouchbasePersistentProperty> provider = getParameterProvider(entity, source, evaluator,
				parent);
		EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);

		final R instance = instantiator.createInstance(entity, provider);
		final ConvertingPropertyAccessor accessor = getPropertyAccessor(instance);

		entity.doWithProperties(new PropertyHandler<>() {
			@Override
			public void doWithPersistentProperty(final CouchbasePersistentProperty prop) {
				if (!doesPropertyExistInSource(prop) || entity.isCreatorArgument(prop) || isIdConstructionProperty(prop)
						|| prop.isAnnotationPresent(N1qlJoin.class)) {
					return;
				}

				Object obj = prop == entity.getIdProperty() && parent == null ? source.getId()
						: getValueInternal(prop, source, instance, entity);

				accessor.setProperty(prop, obj);
			}

			/**
			 * doesPropertyExistInSource. It could have getFieldName() or mangled(getFieldName())
			 *
			 * @param property
			 * @return
			 */
			private boolean doesPropertyExistInSource(final CouchbasePersistentProperty property) {
				return property.isIdProperty() || source.containsKey(property.getFieldName())
						|| source.containsKey(maybeMangle(property));
			}

			private boolean isIdConstructionProperty(final CouchbasePersistentProperty property) {
				return property.isAnnotationPresent(IdPrefix.class) || property.isAnnotationPresent(IdSuffix.class);
			}
		});

		entity.doWithAssociations((AssociationHandler<CouchbasePersistentProperty>) association -> {
			CouchbasePersistentProperty inverseProp = association.getInverse();
			Object obj = getValueInternal(inverseProp, source, instance, entity);
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
			final Object parent, PersistentEntity entity) {
		return new CouchbasePropertyValueProvider(source, expressionEvaluatorFactory.create(source), parent, entity).getPropertyValue(property);
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
			final ValueExpressionEvaluator evaluator, final Object parent) {
		CouchbasePropertyValueProvider provider = new CouchbasePropertyValueProvider(source, evaluator, parent, entity);
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
			// no longer needed with Enum converters
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

	/**
	 * Potentially convert simple values like ENUMs.
	 *
	 * @param value the value to convert.
	 * @param target the target persistent property which may have an Encrypt annotation
	 * @return the potentially converted object.
	 */
	@SuppressWarnings("unchecked")
	protected Object getPotentiallyConvertedSimpleRead(Object value, final CouchbasePersistentProperty target) {
		if (value == null || target == null) {
			return value;
		}
		// this call to convert takes TypeDescriptors - the target type descriptor may have an Encrypt annotation.
		return conversionService.convert(value, TypeDescriptor.forObject(value), new TypeDescriptor(target.getField()));
	}

	@Override
	public void write(final Object source, final CouchbaseDocument target) {
		if (source == null) {
			return;
		}

		boolean isCustom = conversions.getCustomWriteTarget(source.getClass(), CouchbaseDocument.class).isPresent();
		TypeInformation<?> type = TypeInformation.of(source.getClass());

		if (!isCustom) {
			typeMapper.writeType(type, target);
		}

		writeInternalRoot(source, target, type, true, null, true);
		if (target.getId() == null) {
			throw new MappingException("An ID property is needed, but not found/could not be generated on this entity.");
		}
	}

	/**
	 * Convert a source object into a {@link CouchbaseDocument} target.
	 *
	 * @param source the source object.
	 * @param target the target document.
	 * @param withId write out with the id.
	 * @param property will be null for the root
	 */
	@SuppressWarnings("unchecked")
	public void writeInternalRoot(final Object source, CouchbaseDocument target, TypeInformation<?> typeHint,
			boolean withId, CouchbasePersistentProperty property, boolean processValueConverter) {
		if (source == null) {
			return;
		}

		Optional<Class<?>> customTarget = conversions.getCustomWriteTarget(source.getClass(), CouchbaseDocument.class);
		if (customTarget.isPresent()) {
			copyCouchbaseDocument(conversionService.convert(source, CouchbaseDocument.class), target);
			return;
		}

		if (Map.class.isAssignableFrom(source.getClass())) {
			writeMapInternal((Map<Object, Object>) source, target, TypeInformation.MAP, property);
			return;
		}

		if (Collection.class.isAssignableFrom(source.getClass())) {
			throw new IllegalArgumentException("Root Document must be either CouchbaseDocument or Map.");
		}

		CouchbasePersistentEntity<?> entity = mappingContext.getPersistentEntity(source.getClass());
		writeInternalEntity(source, target, entity, withId, property, processValueConverter);
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
	protected void writeInternalEntity(final Object source, final CouchbaseDocument target,
			final CouchbasePersistentEntity<?> entity, boolean withId, CouchbasePersistentProperty prop,
			boolean processValueConverter) {
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

		target.setExpiration((int) (entity.getExpiryDuration().getSeconds()));

		// write all the entity.properties to the target. Does not write the id or version.
		writeToTargetDocument(target, entity, accessor, idProperty, versionProperty, prefixes, suffixes, idAttributes);

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
					writePropertyInternal(propertyObj, target, inverseProp, accessor);
				}
			}
		});

		if (prop != null && processValueConverter && conversions.hasValueConverter(prop)) { // whole entity is encrypted
			Map<String, Object> propertyConverted = (Map<String, Object>) conversions.getPropertyValueConversions()
					.getValueConverter(prop).write(source, new CouchbaseConversionContext(prop, this, accessor));
			target.setContent(JsonObject.from(propertyConverted));
		}
	}

	private void writeToTargetDocument(final CouchbaseDocument target, final CouchbasePersistentEntity<?> entity,
			final ConvertingPropertyAccessor<Object> accessor, final CouchbasePersistentProperty idProperty,
			final CouchbasePersistentProperty versionProperty, final TreeMap<Integer, String> prefixes,
			final TreeMap<Integer, String> suffixes, final TreeMap<Integer, String> idAttributes) {
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

					if (prop.isAnnotationPresent(Transient.class)) {
						return;
					}

					if (!conversions.isSimpleType(propertyObj.getClass())) {
						writePropertyInternal(propertyObj, target, prop, accessor);
					} else {
						writeSimpleInternal(prop, accessor, target, prop.getFieldName());
					}
				}
			}
		});
	}

	/**
	 * Helper method to write a non-simple property into the target document.
	 *
	 * @param source the source object.
	 * @param target the target document.
	 * @param prop the property information.
	 */
	@SuppressWarnings("unchecked")
	protected void writePropertyInternal(final Object source, final CouchbaseDocument target,
			final CouchbasePersistentProperty prop, final ConvertingPropertyAccessor accessor) {
		if (source == null) {
			return;
		}

		String name = prop.getFieldName();
		TypeInformation<?> valueType = TypeInformation.of(source.getClass());
		TypeInformation<?> type = prop.getTypeInformation();
		if (valueType.isCollectionLike()) {
			CouchbaseList collectionDoc = createCollection(asCollection(source), valueType, prop, accessor);
			putMaybeEncrypted(target, prop, collectionDoc, accessor);
			return;
		}

		if (valueType.isMap()) {
			CouchbaseDocument mapDoc = createMap((Map<Object, Object>) source, prop);
			putMaybeEncrypted(target, prop, mapDoc, accessor);
			return;
		}

		if (conversions.hasValueConverter(prop)) { // property is encrypted
			putMaybeEncrypted(target, prop, source, accessor);
			return;
		}

		if (valueType.getType().equals(java.util.Optional.class)) {
			Optional<?> o = (Optional<?>) source;
			writeSimpleInternal(o.map(s -> prop).orElse(null), accessor, target, prop.getFieldName());
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
				: mappingContext.getRequiredPersistentEntity(prop);
		writeInternalEntity(source, propertyDoc, entity, false, prop, true);
		target.put(maybeMangle(prop), propertyDoc);
	}

	private void putMaybeEncrypted(CouchbaseDocument target, CouchbasePersistentProperty prop, Object value,
			ConvertingPropertyAccessor accessor) {
		if (conversions.hasValueConverter(prop)) { // property is encrypted
			value = conversions.getPropertyValueConversions().getValueConverter(prop).write(value,
					new CouchbaseConversionContext(prop, this, accessor));
		}
		target.put(maybeMangle(prop), value);
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

		return writeMapInternal(map, new CouchbaseDocument(), prop.getTypeInformation(), prop);
	}

	/**
	 * Helper method to write the map into the couchbase document.
	 *
	 * @param source the source object.
	 * @param target the target document.
	 * @return the written couchbase document.
	 */
	private CouchbaseDocument writeMapInternal(final Map<? extends Object, Object> source, final CouchbaseDocument target,
			TypeInformation<?> type, CouchbasePersistentProperty prop) {
		for (Map.Entry<? extends Object, Object> entry : source.entrySet()) {
			Object key = entry.getKey();
			Object val = entry.getValue();

			if (conversions.isSimpleType(key.getClass())) {
				String simpleKey = key.toString();

				if (val == null || conversions.isSimpleType(val.getClass())) {
					writeSimpleInternal(val, target, simpleKey); // this is an entry in a map, cannot have an annotation
				} else if (val instanceof Collection || val.getClass().isArray()) {
					target.put(simpleKey,
							writeCollectionInternal(asCollection(val), new CouchbaseList(conversions.getSimpleTypeHolder()),
									prop.getTypeInformation(), prop, getPropertyAccessor(val)));
				} else {
					CouchbaseDocument embeddedDoc = new CouchbaseDocument();
					writeInternalRoot(val, embeddedDoc, prop.getTypeInformation(), false, prop, true);
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
	 * @return the created couchbase list.
	 */
	private CouchbaseList createCollection(final Collection<?> collection, final TypeInformation<?> type,
			CouchbasePersistentProperty prop, ConvertingPropertyAccessor accessor) {
		return writeCollectionInternal(collection, new CouchbaseList(conversions.getSimpleTypeHolder()), type, prop,
				accessor);
	}

	/**
	 * Helper method to write the internal collection.
	 *
	 * @param source the source object.
	 * @param target the target document.
	 * @return the created couchbase list.
	 */
	public CouchbaseList writeCollectionInternal(final Collection<?> source, final CouchbaseList target,
			final TypeInformation<?> type, CouchbasePersistentProperty prop, ConvertingPropertyAccessor accessor) {

		for (Object element : source) {
			Class<?> elementType = element == null ? null : element.getClass();

			if (elementType == null || conversions.isSimpleType(elementType)) {
				target.put(getPotentiallyConvertedSimpleWrite(element));
			} else if (element instanceof Collection || elementType.isArray()) {
				target.put(writeCollectionInternal(asCollection(element), new CouchbaseList(conversions.getSimpleTypeHolder()),
						type, prop, accessor));
			} else {
				CouchbaseDocument embeddedDoc = new CouchbaseDocument();
				writeInternalRoot(element, embeddedDoc,
						prop != null ? prop.getTypeInformation() : TypeInformation.of(elementType), false, prop, true);
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
				items.add(readCollection(componentType != null ? componentType :TypeInformation.of(dbObjItem.getClass()), (CouchbaseList) dbObjItem, parent));
			} else {
				items.add(getPotentiallyConvertedSimpleRead(dbObjItem, rawComponentType));
			}
		}

		return getPotentiallyConvertedSimpleRead(items, targetType.getType());
	}

	/**
	 * Write the given source into the couchbase document target.
	 *
	 * @param source the source object. This does not have access to annotaions.
	 * @param target the target document.
	 * @param key the key of the object.
	 */
	private void writeSimpleInternal(final Object source, final CouchbaseDocument target, final String key) {
		target.put(key, getPotentiallyConvertedSimpleWrite(source));
	}

	/**
	 * Write the given source into the couchbase document target.
	 *
	 * @param source the source persistent property. This has access to annotations.
	 * @param target the target document.
	 * @param key the key of the object.
	 */
	private void writeSimpleInternal(final CouchbasePersistentProperty source,
			final ConvertingPropertyAccessor<Object> accessor, final CouchbaseDocument target, final String key) {
		Object result = getPotentiallyConvertedSimpleWrite(source, accessor);
		if (result instanceof Optional) {
			Optional<Object> optional = (Optional) result;
			result = optional.orElse(null);
		}
		target.put(maybeMangle(source), result);
	}

	public Object getPotentiallyConvertedSimpleWrite(final Object value) {
		return convertForWriteIfNeeded(value); // cannot access annotations
	}

	/**
	 * This does process PropertyValueConversions
	 *
	 * @param value
	 * @param accessor
	 * @return
	 */
	@Deprecated
	public Object getPotentiallyConvertedSimpleWrite(final CouchbasePersistentProperty value,
			ConvertingPropertyAccessor<Object> accessor) {
		return convertForWriteIfNeeded(value, accessor, true); // can access annotations
	}

	/**
	 * This does process PropertyValueConversions
	 *
	 * @param property
	 * @param accessor
	 * @param processValueConverter
	 * @return
	 */
	public Object getPotentiallyConvertedSimpleWrite(final CouchbasePersistentProperty property,
			ConvertingPropertyAccessor<Object> accessor, boolean processValueConverter) {
		return convertForWriteIfNeeded(property, accessor, processValueConverter); // can access annotations
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
		ClassLoader classLoader = applicationContext.getClassLoader();
		if (this.typeMapper instanceof BeanClassLoaderAware && classLoader != null) {
			((BeanClassLoaderAware) this.typeMapper).setBeanClassLoader(classLoader);
		}
	}

	/**
	 * COPIED Set the {@link EntityCallbacks} instance to use when invoking
	 * {@link org.springframework.data.mapping.callback.EntityCallback callbacks} like the {@link AfterConvertCallback}.
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
	private <R> R readValue(Object value, TypeInformation type, Object parent) {
		Class<?> rawType = type.getType();

		if (conversions.hasCustomReadTarget(value.getClass(), rawType)) {
			return (R) conversionService.convert(value, rawType);
		} else if (value instanceof CouchbaseDocument) {
			return (R) read(type, (CouchbaseDocument) value, parent);
		} else if (value instanceof CouchbaseList) {
			return (R) readCollection(type, (CouchbaseList) value, parent);
		} else {
			return (R) getPotentiallyConvertedSimpleRead(value, type.getType()); // type does not have annotations
		}
	}

	/**
	 * Helper method to read the value based on the PersistentProperty
	 *
	 * @param value the value to convert.
	 * @param prop the persistent property - will have annotations (i.e. Encrypt for FLE)
	 * @param parent the optional parent.
	 * @param <R> the target type.
	 * @return the converted object.
	 */
	@SuppressWarnings("unchecked")
	public <R> R readValue(Object value, CouchbasePersistentProperty prop, Object parent, boolean noDecrypt) {
		Class<?> rawType = prop.getType();
		if (conversions.hasValueConverter(prop) && !noDecrypt) {
			try {
				return (R) conversions.getPropertyValueConversions().getValueConverter(prop).read(value,
						new CouchbaseConversionContext(prop, this, null));
			} catch (ConverterHasNoConversion noConversion) {
				; // ignore
			}
		}
		if (conversions.hasCustomReadTarget(value.getClass(), rawType)) {
			TypeInformation ti = TypeInformation.of(value.getClass());
			return (R) conversionService.convert(value, ti.toTypeDescriptor(), new TypeDescriptor(prop.getField()));
		}
		if (value instanceof CouchbaseDocument) {
			return (R) read(prop.getTypeInformation(), (CouchbaseDocument) value, parent);
		}
		if (value instanceof CouchbaseList) {
			return (R) readCollection(prop.getTypeInformation(), (CouchbaseList) value, parent);
		}
		return (R) getPotentiallyConvertedSimpleRead(value, prop);// passes PersistentProperty with annotations

	}

	private ConvertingPropertyAccessor<Object> getPropertyAccessor(Object source) {

		CouchbasePersistentEntity<?> entity = null;
		try {
			entity = mappingContext.getRequiredPersistentEntity(source.getClass());
		} catch(InaccessibleObjectException e){
			try { // punt
				entity = mappingContext.getRequiredPersistentEntity(Object.class);
			} catch(Exception ee){
				throw e;
			}
		}
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
		private final ValueExpressionEvaluator evaluator;

		/**
		 * The optional parent object.
		 */
		private final Object parent;

		/**
		 * The entity of the property
		 */
		private final PersistentEntity entity;

		public CouchbasePropertyValueProvider(final CouchbaseDocument source,
				final ValueExpressionEvaluator evaluator, final Object parent, final PersistentEntity entity) {
			Assert.notNull(source, "CouchbaseDocument must not be null!");
			Assert.notNull(evaluator, "ValueExpressionEvaluator must not be null!");

			this.source = source;
			this.evaluator = evaluator;
			this.parent = parent;
			this.entity = entity;
		}

		@Override
		@SuppressWarnings("unchecked")
		public <R> R getPropertyValue(final CouchbasePersistentProperty property) {
			String expression = property.getSpelExpression();
			String maybeFieldName = maybeMangle(property);
			Object value = expression != null ? evaluator.evaluate(expression) : source.get(maybeFieldName);
			boolean noDecrypt = false;

			// handle @Encrypted FROM_UNENCRYPTED. Just accept them as-is.
			if (property.findAnnotation(Encrypted.class) != null) {
				if (value == null && !maybeFieldName.equals(property.getFieldName())
						&& property.findAnnotation(Encrypted.class).migration().equals(Encrypted.Migration.FROM_UNENCRYPTED)) {
					value = source.get(property.getFieldName());
					noDecrypt = true;
				} else if (value != null
						&& !((value instanceof CouchbaseDocument) && (((CouchbaseDocument) value)).containsKey("kid"))) {
					noDecrypt = true;
					// TODO - should we throw an exception, or just ignore the problem of not being encrypted with noDecrypt=true?
					throw new RuntimeException("should have been encrypted, but is not " + maybeFieldName);
				}
			}

			if (property == entity.getIdProperty() && parent == null) {
				return readValue(source.getId(), property.getTypeInformation(), source);
			}
			if (value == null) {
				return null;
			}
			return readValue(value, property, source, noDecrypt);
		}
	}

	public String maybeMangle(PersistentProperty<?> property) {
		Assert.notNull(property, "property");
		if (!conversions.hasValueConverter(property)) {
			return ((CouchbasePersistentProperty) property).getFieldName();
		}
		PropertyValueConverter<?, ?, ?> propertyValueConverter = conversions.getPropertyValueConversions()
				.getValueConverter((CouchbasePersistentProperty) property);
		CryptoManager cryptoManager = propertyValueConverter != null && propertyValueConverter instanceof CryptoConverter
				? ((CryptoConverter) propertyValueConverter).cryptoManager()
				: null;
		String fname = ((CouchbasePersistentProperty) property).getFieldName();
		return cryptoManager != null ? cryptoManager.mangle(fname) : fname;
	}

	/**
	 * A expression parameter value provider.
	 */
	private class ConverterAwareSpELExpressionParameterValueProvider
			extends ValueExpressionParameterValueProvider<CouchbasePersistentProperty> {

		private final Object parent;

		public ConverterAwareSpELExpressionParameterValueProvider(final ValueExpressionEvaluator evaluator,
				final ConversionService conversionService, final ParameterValueProvider<CouchbasePersistentProperty> delegate,
				final Object parent) {
			super(evaluator, conversionService, delegate);
			this.parent = parent;
		}

		@Override
		protected <T> T potentiallyConvertExpressionValue(Object object, Parameter<T, CouchbasePersistentProperty> parameter) {
			return readValue(object, parameter.getType(), parent);
		}
	}
}
