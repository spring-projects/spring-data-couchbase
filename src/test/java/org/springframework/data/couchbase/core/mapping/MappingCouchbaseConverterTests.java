/*
 * Copyright 2013-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.mapping;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.data.Offset.offset;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.couchbase.core.convert.CouchbaseCustomConversions;
import org.springframework.data.couchbase.core.convert.CouchbaseJsr310Converters.LocalDateTimeToLongConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.mapping.MappingException;

/**
 * @author Michael Nitschinger
 * @author Geoffrey Mina
 * @author Subhashni Balakrishnan
 * @author Mark Paluch
 */
public class MappingCouchbaseConverterTests {

	private static MappingCouchbaseConverter converter = new MappingCouchbaseConverter();

	static {
		converter.afterPropertiesSet();
	}

	@Test
	void shouldNotThrowNPE() {
		CouchbaseDocument converted = new CouchbaseDocument();
		converter.write(null, converted);
		assertThat(converted.getId()).isNull();
		assertThat(converted.getExpiration()).isEqualTo(0);
	}

	@Test
	void doesNotAllowSimpleType1() {
		assertThrows(MappingException.class, () -> converter.write("hello", new CouchbaseDocument()));
	}

	@Test
	void doesNotAllowSimpleType2() {
		assertThrows(MappingException.class, () -> converter.write(true, new CouchbaseDocument()));
	}

	@Test
	void doesNotAllowSimpleType3() {
		assertThrows(MappingException.class, () -> converter.write(42, new CouchbaseDocument()));
	}

	@Test
	void needsIDOnEntity() {
		assertThrows(MappingException.class, () -> converter.write(new EntityWithoutID("foo"), new CouchbaseDocument()));
	}

	@Test
	void writesString() {
		CouchbaseDocument converted = new CouchbaseDocument();
		StringEntity entity = new StringEntity("foobar");

		converter.write(entity, converted);
		Map<String, Object> result = converted.export();
		assertThat(result.get("_class")).isEqualTo(entity.getClass().getName());
		assertThat(result.get("attr0")).isEqualTo("foobar");
		assertThat(converted.getId()).isEqualTo(BaseEntity.ID);
	}

	@Test
	void readsString() {
		CouchbaseDocument source = new CouchbaseDocument();
		source.put("_class", StringEntity.class.getName());
		source.put("attr0", "foobar");

		StringEntity converted = converter.read(StringEntity.class, source);
		assertThat(converted.attr0).isEqualTo("foobar");
	}

	@Test
	void writesNumber() {
		CouchbaseDocument converted = new CouchbaseDocument();
		NumberEntity entity = new NumberEntity(42);

		converter.write(entity, converted);
		Map<String, Object> result = converted.export();
		assertThat(result.get("_class")).isEqualTo(entity.getClass().getName());
		assertThat(result.get("attr0")).isEqualTo(42L);
		assertThat(converted.getId()).isEqualTo(BaseEntity.ID);
	}

	@Test
	void readsNumber() {
		CouchbaseDocument source = new CouchbaseDocument();
		source.put("_class", NumberEntity.class.getName());
		source.put("attr0", 42);

		NumberEntity converted = converter.read(NumberEntity.class, source);
		assertThat(converted.attr0).isEqualTo(42);
	}

	@Test
	void writesBoolean() {
		CouchbaseDocument converted = new CouchbaseDocument();
		BooleanEntity entity = new BooleanEntity(true);

		converter.write(entity, converted);
		Map<String, Object> result = converted.export();
		assertThat(result.get("_class")).isEqualTo(entity.getClass().getName());
		assertThat(result.get("attr0")).isEqualTo(true);
		assertThat(converted.getId()).isEqualTo("mockid");
	}

	@Test
	void readsBoolean() {
		CouchbaseDocument source = new CouchbaseDocument();
		source.put("_class", BooleanEntity.class.getName());
		source.put("attr0", true);

		BooleanEntity converted = converter.read(BooleanEntity.class, source);
		assertThat(converted.attr0).isTrue();
	}

	@Test
	void writesMixedSimpleTypes() {
		CouchbaseDocument converted = new CouchbaseDocument();
		MixedSimpleEntity entity = new MixedSimpleEntity("a", 5, -0.3, true);

		converter.write(entity, converted);
		Map<String, Object> result = converted.export();
		assertThat(result.get("_class")).isEqualTo(entity.getClass().getName());
		assertThat(result.get("attr0")).isEqualTo("a");
		assertThat(result.get("attr1")).isEqualTo(5);
		assertThat(result.get("attr2")).isEqualTo(-0.3);
		assertThat(result.get("attr3")).isEqualTo(true);
	}

	@Test
	void readsMixedSimpleTypes() {
		CouchbaseDocument source = new CouchbaseDocument();
		source.put("_class", MixedSimpleEntity.class.getName());
		source.put("attr0", "a");
		source.put("attr1", 5);
		source.put("attr2", -0.3);
		source.put("attr3", true);

		MixedSimpleEntity converted = converter.read(MixedSimpleEntity.class, source);
		assertThat(converted.attr0).isEqualTo("a");
		assertThat(converted.attr1).isEqualTo(5);
		assertThat(converted.attr2).isCloseTo(-0.3, offset(0.0));
		assertThat(converted.attr3).isTrue();
	}

	@Test
	void readsID() {
		CouchbaseDocument document = new CouchbaseDocument("001");
		User user = converter.read(User.class, document);
		assertThat(user.getId()).isEqualTo("001");
	}

	@Test
	void writesUninitializedValues() {
		CouchbaseDocument converted = new CouchbaseDocument();
		UninitializedEntity entity = new UninitializedEntity();

		converter.write(entity, converted);
		Map<String, Object> result = converted.export();
		assertThat(result.get("_class")).isEqualTo(entity.getClass().getName());
		assertThat(result.get("attr1")).isEqualTo(0);
	}

	@Test
	void readsUninitializedValues() {
		CouchbaseDocument source = new CouchbaseDocument();
		source.put("_class", UninitializedEntity.class.getName());
		source.put("attr1", 0);

		UninitializedEntity converted = converter.read(UninitializedEntity.class, source);
		assertThat(converted.attr0).isNull();
		assertThat(converted.attr1).isEqualTo(0);
		assertThat(converted.attr2).isNull();
	}

	@Test
	void writesAndReadsMapsAndNestedMaps() {
		CouchbaseDocument converted = new CouchbaseDocument();

		Map<String, String> attr0 = new HashMap<>();
		Map<String, Boolean> attr1 = new TreeMap<>();
		Map<Integer, String> attr2 = new LinkedHashMap<>();
		Map<String, Map<String, String>> attr3 = new HashMap<>();

		attr0.put("foo", "bar");
		attr1.put("bar", true);
		attr3.put("hashmap", attr0);

		MapEntity entity = new MapEntity(attr0, attr1, attr2, attr3);

		converter.write(entity, converted);
		Map<String, Object> result = converted.export();
		assertThat(result.get("attr0")).isEqualTo(attr0);
		assertThat(result.get("attr1")).isEqualTo(attr1);
		assertThat(result.get("attr2")).isEqualTo(attr2);
		assertThat(result.get("attr3")).isEqualTo(attr3);

		CouchbaseDocument cattr0 = new CouchbaseDocument();
		cattr0.put("foo", "bar");

		CouchbaseDocument cattr1 = new CouchbaseDocument();
		cattr1.put("bar", true);

		CouchbaseDocument cattr2 = new CouchbaseDocument();

		CouchbaseDocument cattr3 = new CouchbaseDocument();
		cattr3.put("hashmap", cattr0);

		CouchbaseDocument source = new CouchbaseDocument();
		source.put("_class", MapEntity.class.getName());
		source.put("attr0", cattr0);
		source.put("attr1", cattr1);
		source.put("attr2", cattr2);
		source.put("attr3", cattr3);

		MapEntity readConverted = converter.read(MapEntity.class, source);
		assertThat(readConverted.attr0).isEqualTo(attr0);
		assertThat(readConverted.attr1).isEqualTo(attr1);
		assertThat(readConverted.attr2).isEqualTo(attr2);
		assertThat(readConverted.attr3).isEqualTo(attr3);
	}

	@Test
	void writesAndReadsListAndNestedList() {
		CouchbaseDocument converted = new CouchbaseDocument();
		List<String> attr0 = new ArrayList<>();
		List<Integer> attr1 = new LinkedList<>();
		List<List<String>> attr2 = new ArrayList<>();

		attr0.add("foo");
		attr0.add("bar");
		attr2.add(attr0);

		ListEntity entity = new ListEntity(attr0, attr1, attr2);

		converter.write(entity, converted);
		Map<String, Object> result = converted.export();
		assertThat(result.get("attr0")).isEqualTo(attr0);
		assertThat(result.get("attr1")).isEqualTo(attr1);
		assertThat(result.get("attr2")).isEqualTo(attr2);

		CouchbaseDocument source = new CouchbaseDocument();
		source.put("_class", ListEntity.class.getName());
		CouchbaseList cattr0 = new CouchbaseList();
		cattr0.put("foo");
		cattr0.put("bar");
		CouchbaseList cattr1 = new CouchbaseList();
		CouchbaseList cattr2 = new CouchbaseList();
		cattr2.put(cattr0);
		source.put("attr0", cattr0);
		source.put("attr1", cattr1);
		source.put("attr2", cattr2);

		ListEntity readConverted = converter.read(ListEntity.class, source);
		assertThat(readConverted.attr0.size()).isEqualTo(2);
		assertThat(readConverted.attr1.size()).isEqualTo(0);
		assertThat(readConverted.attr2.size()).isEqualTo(1);
		assertThat(readConverted.attr2.get(0).size()).isEqualTo(2);
	}

	@Test
	void writesAndReadsSetAndNestedSet() {
		CouchbaseDocument converted = new CouchbaseDocument();
		Set<String> attr0 = new HashSet<>();
		TreeSet<Integer> attr1 = new TreeSet<>();
		Set<Set<String>> attr2 = new HashSet<>();

		attr0.add("foo");
		attr0.add("bar");
		attr2.add(attr0);

		SetEntity entity = new SetEntity(attr0, attr1, attr2);

		converter.write(entity, converted);
		Map<String, Object> result = converted.export();
		assertThat(((Collection) result.get("attr0")).size()).isEqualTo(attr0.size());
		assertThat(((Collection) result.get("attr1")).size()).isEqualTo(attr1.size());
		assertThat(((Collection) result.get("attr2")).size()).isEqualTo(attr2.size());

		CouchbaseList cattr0 = new CouchbaseList();
		cattr0.put("foo");
		cattr0.put("bar");

		CouchbaseList cattr1 = new CouchbaseList();

		CouchbaseList cattr2 = new CouchbaseList();
		cattr2.put(cattr0);

		CouchbaseDocument source = new CouchbaseDocument();
		source.put("_class", SetEntity.class.getName());
		source.put("attr0", cattr0);
		source.put("attr1", cattr1);
		source.put("attr2", cattr2);

		SetEntity readConverted = converter.read(SetEntity.class, source);
		assertThat(readConverted.attr0).isEqualTo(attr0);
		assertThat(readConverted.attr1).isEqualTo(attr1);
		assertThat(readConverted.attr2).isEqualTo(attr2);
	}

	@Test
	void writesAndReadsValueClass() {
		CouchbaseDocument converted = new CouchbaseDocument();

		final String email = "foo@bar.com";
		final Email addy = new Email(email);
		List<Email> listOfEmails = new ArrayList<Email>();
		listOfEmails.add(addy);

		ValueEntity entity = new ValueEntity(addy, listOfEmails);
		converter.write(entity, converted);
		Map<String, Object> result = converted.export();

		assertThat(result.get("_class")).isEqualTo(entity.getClass().getName());
		assertThat(result.get("email")).isEqualTo(new HashMap<String, Object>() {
			{
				put("emailAddr", email);
			}
		});

		CouchbaseDocument source = new CouchbaseDocument();
		source.put("_class", ValueEntity.class.getName());
		CouchbaseDocument emailDoc = new CouchbaseDocument();
		emailDoc.put("emailAddr", "foo@bar.com");
		source.put("email", emailDoc);
		CouchbaseList listOfEmailsDoc = new CouchbaseList();
		listOfEmailsDoc.put(emailDoc);
		source.put("listOfEmails", listOfEmailsDoc);

		ValueEntity readConverted = converter.read(ValueEntity.class, source);
		assertThat(readConverted.email.emailAddr).isEqualTo(addy.emailAddr);
		assertThat(readConverted.listOfEmails.get(0).emailAddr).isEqualTo(listOfEmails.get(0).emailAddr);
	}

	@Test
	void writesAndReadsCustomConvertedClass() {
		List<Object> converters = new ArrayList<>();
		converters.add(BigDecimalToStringConverter.INSTANCE);
		converters.add(StringToBigDecimalConverter.INSTANCE);
		converter.setCustomConversions(new CouchbaseCustomConversions(converters));
		converter.afterPropertiesSet();

		CouchbaseDocument converted = new CouchbaseDocument();

		final String valueStr = "12.345";
		final BigDecimal value = new BigDecimal(valueStr);
		final String value2Str = "0.6789";
		final BigDecimal value2 = new BigDecimal(value2Str);
		List<BigDecimal> listOfValues = new ArrayList<>();
		listOfValues.add(value);
		listOfValues.add(value2);
		Map<String, BigDecimal> mapOfValues = new HashMap<>();
		mapOfValues.put("val1", value);
		mapOfValues.put("val2", value2);

		CustomEntity entity = new CustomEntity(value, listOfValues, mapOfValues);
		converter.write(entity, converted);

		CouchbaseDocument source = new CouchbaseDocument();
		source.put("_class", CustomEntity.class.getName());
		source.put("value", valueStr);
		CouchbaseList listOfValuesDoc = new CouchbaseList();
		listOfValuesDoc.put(valueStr);
		listOfValuesDoc.put(value2Str);
		source.put("listOfValues", listOfValuesDoc);
		CouchbaseDocument mapOfValuesDoc = new CouchbaseDocument();
		mapOfValuesDoc.put("val1", valueStr);
		mapOfValuesDoc.put("val2", value2Str);
		source.put("mapOfValues", mapOfValuesDoc);

		assertThat(valueStr).isEqualTo(((CouchbaseList) converted.getPayload().get("listOfValues")).get(0));
		assertThat(value2Str).isEqualTo(((CouchbaseList) converted.getPayload().get("listOfValues")).get(1));
		assertThat(converted.export().toString()).isEqualTo(source.export().toString());

		CustomEntity readConverted = converter.read(CustomEntity.class, source);
		assertThat(readConverted.value).isEqualTo(value);
		assertThat(readConverted.listOfValues.get(0)).isEqualTo(listOfValues.get(0));
		assertThat(readConverted.listOfValues.get(1)).isEqualTo(listOfValues.get(1));
		assertThat(readConverted.mapOfValues.get("val1")).isEqualTo(mapOfValues.get("val1"));
		assertThat(readConverted.mapOfValues.get("val2")).isEqualTo(mapOfValues.get("val2"));
	}

	@Test
	void writesAndReadsClassContainingCustomConvertedObjects() {
		List<Object> converters = new ArrayList<>();
		converters.add(BigDecimalToStringConverter.INSTANCE);
		converters.add(StringToBigDecimalConverter.INSTANCE);
		converter.setCustomConversions(new CouchbaseCustomConversions(converters));
		converter.afterPropertiesSet();

		CouchbaseDocument converted = new CouchbaseDocument();

		final String weightStr = "12.34";
		final BigDecimal weight = new BigDecimal(weightStr);
		final CustomObject addy = new CustomObject(weight);
		List<CustomObject> listOfObjects = new ArrayList<>();
		listOfObjects.add(addy);
		Map<String, CustomObject> mapOfObjects = new HashMap<>();
		mapOfObjects.put("obj0", addy);
		mapOfObjects.put("obj1", addy);

		CustomObjectEntity entity = new CustomObjectEntity(addy, listOfObjects, mapOfObjects);
		converter.write(entity, converted);

		CouchbaseDocument source = new CouchbaseDocument();
		source.put("_class", CustomObjectEntity.class.getName());
		CouchbaseDocument objectDoc = new CouchbaseDocument();
		objectDoc.put("weight", weightStr);
		source.put("object", objectDoc);
		CouchbaseList listOfObjectsDoc = new CouchbaseList();
		listOfObjectsDoc.put(objectDoc);
		source.put("listOfObjects", listOfObjectsDoc);
		CouchbaseDocument mapOfObjectsDoc = new CouchbaseDocument();
		mapOfObjectsDoc.put("obj0", objectDoc);
		mapOfObjectsDoc.put("obj1", objectDoc);
		source.put("mapOfObjects", mapOfObjectsDoc);
		assertThat(converted.export().toString()).isEqualTo(source.export().toString());

		CustomObjectEntity readConverted = converter.read(CustomObjectEntity.class, source);
		assertThat(readConverted.object.weight).isEqualTo(addy.weight);
		assertThat(readConverted.listOfObjects.get(0).weight).isEqualTo(listOfObjects.get(0).weight);
		assertThat(readConverted.mapOfObjects.get("obj0").weight).isEqualTo(mapOfObjects.get("obj0").weight);
		assertThat(readConverted.mapOfObjects.get("obj1").weight).isEqualTo(mapOfObjects.get("obj1").weight);
	}

	@Test
	void writesAndReadsDates() {
		Date created = new Date();
		Calendar modified = Calendar.getInstance();
		LocalDateTime deleted = LocalDateTime.now();
		DateEntity entity = new DateEntity(created, modified, deleted);

		CouchbaseDocument converted = new CouchbaseDocument();
		converter.write(entity, converted);
		assertThat(converted.getPayload().get("created")).isEqualTo(created.getTime());
		assertThat(converted.getPayload().get("modified")).isEqualTo(modified.getTimeInMillis() / 1000);
		LocalDateTimeToLongConverter localDateTimeToDateconverter = LocalDateTimeToLongConverter.INSTANCE;
		assertThat(converted.getPayload().get("deleted")).isEqualTo(localDateTimeToDateconverter.convert(deleted));

		DateEntity read = converter.read(DateEntity.class, converted);
		assertThat(read.created.getTime()).isEqualTo(created.getTime());
		assertThat(read.modified.getTimeInMillis() / 1000).isEqualTo(modified.getTimeInMillis() / 1000);
		assertThat(read.deleted.truncatedTo(ChronoUnit.MILLIS)).isEqualTo(deleted.truncatedTo(ChronoUnit.MILLIS));
	}

	@WritingConverter
	public enum BigDecimalToStringConverter implements Converter<BigDecimal, String> {
		INSTANCE;

		@Override
		public String convert(BigDecimal source) {
			return source.toPlainString();
		}
	}

	@ReadingConverter
	public enum StringToBigDecimalConverter implements Converter<String, BigDecimal> {
		INSTANCE;

		@Override
		public BigDecimal convert(String source) {
			return new BigDecimal(source);
		}
	}

	static class EntityWithoutID {

		private String attr0;

		public EntityWithoutID(String a0) {
			attr0 = a0;
		}
	}

	static class BaseEntity {
		public static final String ID = "mockid";
		@Id private String id = ID;
	}

	static class StringEntity extends BaseEntity {
		private String attr0;

		public StringEntity(String attr0) {
			this.attr0 = attr0;
		}
	}

	static class NumberEntity extends BaseEntity {
		private long attr0;

		public NumberEntity(long attr0) {
			this.attr0 = attr0;
		}
	}

	static class BooleanEntity extends BaseEntity {
		private boolean attr0;

		public BooleanEntity(boolean attr0) {
			this.attr0 = attr0;
		}
	}

	static class MixedSimpleEntity extends BaseEntity {
		private String attr0;
		private int attr1;
		private double attr2;
		private boolean attr3;

		public MixedSimpleEntity(String attr0, int attr1, double attr2, boolean attr3) {
			this.attr0 = attr0;
			this.attr1 = attr1;
			this.attr2 = attr2;
			this.attr3 = attr3;
		}
	}

	static class UninitializedEntity extends BaseEntity {
		private String attr0 = null;
		private int attr1;
		private Integer attr2;
	}

	static class MapEntity extends BaseEntity {
		private Map<String, String> attr0;
		private Map<String, Boolean> attr1;
		private Map<Integer, String> attr2;
		private Map<String, Map<String, String>> attr3;

		public MapEntity(Map<String, String> attr0, Map<String, Boolean> attr1, Map<Integer, String> attr2,
				Map<String, Map<String, String>> attr3) {
			this.attr0 = attr0;
			this.attr1 = attr1;
			this.attr2 = attr2;
			this.attr3 = attr3;
		}
	}

	static class ListEntity extends BaseEntity {
		private List<String> attr0;
		private List<Integer> attr1;
		private List<List<String>> attr2;

		ListEntity(List<String> attr0, List<Integer> attr1, List<List<String>> attr2) {
			this.attr0 = attr0;
			this.attr1 = attr1;
			this.attr2 = attr2;
		}
	}

	static class SetEntity extends BaseEntity {
		private Set<String> attr0;
		private Set<Integer> attr1;
		private Set<Set<String>> attr2;

		SetEntity(Set<String> attr0, Set<Integer> attr1, Set<Set<String>> attr2) {
			this.attr0 = attr0;
			this.attr1 = attr1;
			this.attr2 = attr2;
		}
	}

	static class ValueEntity extends BaseEntity {
		private Email email;
		private List<Email> listOfEmails;

		public ValueEntity(Email email, List<Email> listOfEmails) {
			this.email = email;
			this.listOfEmails = listOfEmails;
		}
	}

	static class Email {
		private String emailAddr;

		public Email(String emailAddr) {
			this.emailAddr = emailAddr;
		}
	}

	static class CustomEntity extends BaseEntity {
		private BigDecimal value;
		private List<BigDecimal> listOfValues;
		private Map<String, BigDecimal> mapOfValues;

		public CustomEntity(BigDecimal value, List<BigDecimal> listOfValues, Map<String, BigDecimal> mapOfValues) {
			this.value = value;
			this.listOfValues = listOfValues;
			this.mapOfValues = mapOfValues;
		}
	}

	static class CustomObjectEntity extends BaseEntity {
		private CustomObject object;
		private List<CustomObject> listOfObjects;
		private Map<String, CustomObject> mapOfObjects;

		public CustomObjectEntity(CustomObject object, List<CustomObject> listOfObjects,
				Map<String, CustomObject> mapOfObjects) {
			this.object = object;
			this.listOfObjects = listOfObjects;
			this.mapOfObjects = mapOfObjects;
		}
	}

	static class CustomObject {
		private BigDecimal weight;

		public CustomObject(BigDecimal weight) {
			this.weight = weight;
		}
	}

	static class DateEntity extends BaseEntity {
		private Date created;
		private Calendar modified;
		private LocalDateTime deleted;

		public DateEntity(Date created, Calendar modified, LocalDateTime deleted) {
			this.created = created;
			this.modified = modified;
			this.deleted = deleted;
		}
	}

}
