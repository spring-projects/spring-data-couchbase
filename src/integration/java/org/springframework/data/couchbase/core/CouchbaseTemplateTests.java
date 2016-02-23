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

package org.springframework.data.couchbase.core;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.Document;
import com.couchbase.client.java.repository.annotation.Field;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Anastasiia Smirnova */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
@TestExecutionListeners(CouchbaseTemplateViewListener.class)
public class CouchbaseTemplateTests {

	@Autowired
	private Bucket client;

	@Autowired
	private CouchbaseTemplate template;

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private void removeIfExist(String key) {
		try {
			client.remove(key);
		}
		catch (DocumentDoesNotExistException e) {
			//ignore
		}
	}

	@Test
	public void saveSimpleEntityCorrectly() throws Exception {
		String id = "beers:awesome-stout";
		String name = "The Awesome Stout";
		boolean active = false;
		Beer beer = new Beer(id).setName(name).setActive(active);

		template.save(beer);
		RawJsonDocument resultDoc = client.get(id, RawJsonDocument.class);
		assertNotNull(resultDoc);
		String result = resultDoc.content();
		assertNotNull(result);
		Map<String, Object> resultConv = MAPPER.readValue(result, new TypeReference<Map<String, Object>>() {});

		assertNotNull(resultConv.get(MappingCouchbaseConverter.TYPEKEY_DEFAULT));
		assertNull(resultConv.get("javaClass"));
		assertEquals("org.springframework.data.couchbase.core.Beer", resultConv.get(MappingCouchbaseConverter.TYPEKEY_DEFAULT));
		assertEquals(false, resultConv.get("is_active"));
		assertEquals("The Awesome Stout", resultConv.get("name"));
	}

	@Test
	public void saveDocumentWithExpiry() throws Exception {
		String id = "simple-doc-with-expiry";
		DocumentWithExpiry doc = new DocumentWithExpiry(id);
		template.save(doc);
		assertNotNull(client.get(id));
		Thread.sleep(3000);
		assertNull(client.get(id));
	}

	@Test
	public void insertDoesNotOverride() throws Exception {
		String id = "double-insert-test";
		removeIfExist(id);

		SimplePerson doc = new SimplePerson(id, "Mr. A");
		template.insert(doc);
		RawJsonDocument resultDoc = client.get(id, RawJsonDocument.class);
		assertNotNull(resultDoc);
		String result = resultDoc.content();

		Map<String, String> resultConv = MAPPER.readValue(result, new TypeReference<Map<String, String>>() {});
		assertEquals("Mr. A", resultConv.get("name"));

		doc = new SimplePerson(id, "Mr. B");
		template.insert(doc);

		resultDoc = client.get(id, RawJsonDocument.class);
		assertNotNull(resultDoc);
		result = resultDoc.content();

		resultConv = MAPPER.readValue(result, new TypeReference<Map<String, String>>() {});
		assertEquals("Mr. A", resultConv.get("name"));
	}


	@Test
	public void updateDoesNotInsert() {
		String id = "update-does-not-insert";
		SimplePerson doc = new SimplePerson(id, "Nice Guy");
		template.update(doc);
		assertNull(client.get(id));
	}


	@Test
	public void removeDocument() {
		String id = "beers:to-delete-stout";
		Beer beer = new Beer(id);

		template.save(beer);
		Object result = client.get(id);
		assertNotNull(result);

		template.remove(beer);
		result = client.get(id);
		assertNull(result);
	}


	@Test
	public void storeListsAndMaps() {
		String id = "persons:lots-of-names";
		List<String> names = new ArrayList<String>();
		names.add("Michael");
		names.add("Thomas");
		names.add(null);
		List<Integer> votes = new LinkedList<Integer>();
		Map<String, Boolean> info1 = new HashMap<String, Boolean>();
		info1.put("foo", true);
		info1.put("bar", false);
		info1.put("nullValue", null);
		Map<String, Integer> info2 = new HashMap<String, Integer>();

		ComplexPerson complex = new ComplexPerson(id, names, votes, info1, info2);

		template.save(complex);
		assertNotNull(client.get(id));

		ComplexPerson response = template.findById(id, ComplexPerson.class);
		assertEquals(names, response.getFirstnames());
		assertEquals(votes, response.getVotes());
		assertEquals(id, response.getId());
		assertEquals(info1, response.getInfo1());
		assertEquals(info2, response.getInfo2());
	}


	@Test
	public void validFindById() {
		String id = "beers:findme-stout";
		String name = "The Findme Stout";
		boolean active = true;
		Beer beer = new Beer(id).setName(name).setActive(active);
		template.save(beer);

		Beer found = template.findById(id, Beer.class);

		assertNotNull(found);
		assertEquals(id, found.getId());
		assertEquals(name, found.getName());
		assertEquals(active, found.getActive());
	}

	@Test
	public void shouldLoadAndMapViewDocs() {
		ViewQuery query = ViewQuery.from("test_beers", "by_name");
		query.stale(Stale.FALSE);

		final List<Beer> beers = template.findByView(query, Beer.class);
		assertTrue(beers.size() > 0);

		for (Beer beer : beers) {
			assertNotNull(beer.getId());
			assertNotNull(beer.getName());
			assertNotNull(beer.getActive());
		}
	}

	@Test
	public void shouldQueryRaw() {
		N1qlQuery query = N1qlQuery.simple(select("name").from(i(client.name()))
				.where(x("name").isNotMissing()));

		N1qlQueryResult queryResult = template.queryN1QL(query);
		assertNotNull(queryResult);
		assertTrue(queryResult.errors().toString(), queryResult.finalSuccess());
		assertFalse(queryResult.allRows().isEmpty());
	}

	@Test
	public void shouldQueryWithMapping() {
		FullFragment ff1 = new FullFragment("fullFragment1", 1, "fullFragment", "test1");
		FullFragment ff2 = new FullFragment("fullFragment2", 2, "fullFragment", "test2");
		template.save(Arrays.asList(ff1, ff2));

		N1qlQuery query = N1qlQuery.simple(select(i("value")) //"value" is a n1ql keyword apparently
						.from(i(client.name()))
						.where(x("type").eq(s("fullFragment"))
								.and(x("criteria").gt(1))),

				N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS));

		List<Fragment> fragments = template.findByN1QLProjection(query, Fragment.class);
		assertNotNull(fragments);
		assertFalse(fragments.isEmpty());
		assertEquals(1, fragments.size());
		assertEquals("test2", fragments.get(0).value);
	}

	/**
	 * @see DATACOUCH-159
	 */
	@Test
	public void shouldDeserialiseLongsAndInts() {
		final long longValue = new Date().getTime();
		final int intValue = new Random().nextInt();

		template.save(new SimpleWithLongAndInt("simpleWithLong:simple", longValue, intValue));
		SimpleWithLongAndInt document = template.findById("simpleWithLong:simple", SimpleWithLongAndInt.class);
		assertNotNull(document);
		assertEquals(longValue, document.getLongValue());
		assertEquals(intValue, document.getIntValue());

		template.save(new SimpleWithLongAndInt("simpleWithLong:simple:other", intValue, intValue));
		document = template.findById("simpleWithLong:simple:other", SimpleWithLongAndInt.class);
		assertNotNull(document);
		assertEquals(intValue, document.getLongValue());
		assertEquals(intValue, document.getIntValue());
	}

	@Test
	public void shouldDeserialiseEnums() {
		SimpleWithEnum simpleWithEnum = new SimpleWithEnum("simpleWithEnum:enum", SimpleWithEnum.Type.BIG);
		template.save(simpleWithEnum);
		simpleWithEnum = template.findById("simpleWithEnum:enum", SimpleWithEnum.class);
		assertNotNull(simpleWithEnum);
		assertEquals(simpleWithEnum.getType(), SimpleWithEnum.Type.BIG);
	}

	@Test
	public void shouldDeserialiseClass() {
		SimpleWithClass simpleWithClass = new SimpleWithClass("simpleWithClass:class", Integer.class);
		simpleWithClass.setValue("The dish ran away with the spoon.");
		template.save(simpleWithClass);
		simpleWithClass = template.findById("simpleWithClass:class", SimpleWithClass.class);
		assertNotNull(simpleWithClass);
		assertThat(simpleWithClass.getValue(), equalTo("The dish ran away with the spoon."));
	}

	@Test
	public void shouldHandleCASVersionOnInsert() throws Exception {
		removeIfExist("versionedClass:1");

		VersionedClass versionedClass = new VersionedClass("versionedClass:1", "foobar");
		assertEquals(0, versionedClass.getVersion());
		template.insert(versionedClass);
		RawJsonDocument rawStored = client.get("versionedClass:1", RawJsonDocument.class);
		assertEquals(rawStored.cas(), versionedClass.getVersion());
	}

	@Test
	public void versionShouldNotUpdateOnSecondInsert() throws Exception {
		removeIfExist("versionedClass:2");

		VersionedClass versionedClass = new VersionedClass("versionedClass:2", "foobar");
		template.insert(versionedClass);
		long version1 = versionedClass.getVersion();
		template.insert(versionedClass);
		long version2 = versionedClass.getVersion();

		assertTrue(version1 > 0);
		assertTrue(version2 > 0);
		assertEquals(version1, version2);
	}

	@Test
	public void shouldSaveDocumentOnMatchingVersion() throws Exception {
		removeIfExist("versionedClass:3");

		VersionedClass versionedClass = new VersionedClass("versionedClass:3", "foobar");
		template.insert(versionedClass);
		long version1 = versionedClass.getVersion();

		versionedClass.setField("foobar2");
		template.save(versionedClass);
		long version2 = versionedClass.getVersion();

		assertTrue(version1 > 0);
		assertTrue(version2 > 0);
		assertNotEquals(version1, version2);

		assertEquals("foobar2", template.findById("versionedClass:3", VersionedClass.class).getField());
	}

	@Test(expected = OptimisticLockingFailureException.class)
	public void shouldNotSaveDocumentOnNotMatchingVersion() throws Exception {
		removeIfExist("versionedClass:4");

		VersionedClass versionedClass = new VersionedClass("versionedClass:4", "foobar");
		template.insert(versionedClass);

		RawJsonDocument toCompare = RawJsonDocument.create("versionedClass:4", "different");
		assertNotNull(client.upsert(toCompare));

		versionedClass.setField("foobar2");
		//save (aka upsert) won't error in case of CAS mismatch anymore
		template.update(versionedClass);
	}

	@Test
	public void shouldUpdateDocumentOnMatchingVersion() throws Exception {
		removeIfExist("versionedClass:5");

		VersionedClass versionedClass = new VersionedClass("versionedClass:5", "foobar");
		template.insert(versionedClass);
		long version1 = versionedClass.getVersion();

		versionedClass.setField("foobar2");
		template.update(versionedClass);
		long version2 = versionedClass.getVersion();

		assertTrue(version1 > 0);
		assertTrue(version2 > 0);
		assertNotEquals(version1, version2);

		assertEquals("foobar2", template.findById("versionedClass:5", VersionedClass.class).getField());
	}

	@Test(expected = OptimisticLockingFailureException.class)
	public void shouldNotUpdateDocumentOnNotMatchingVersion() throws Exception {
		removeIfExist("versionedClass:6");

		VersionedClass versionedClass = new VersionedClass("versionedClass:6", "foobar");
		template.insert(versionedClass);

		RawJsonDocument toCompare = RawJsonDocument.create("versionedClass:6", "different");
		assertNotNull(client.upsert(toCompare));

		versionedClass.setField("foobar2");
		template.update(versionedClass);
	}

	@Test
	public void shouldLoadVersionPropertyOnFind() throws Exception {
		removeIfExist("versionedClass:7");

		VersionedClass versionedClass = new VersionedClass("versionedClass:7", "foobar");
		template.insert(versionedClass);
		assertTrue(versionedClass.getVersion() > 0);

		VersionedClass foundClass = template.findById("versionedClass:7", VersionedClass.class);
		assertEquals(versionedClass.getVersion(), foundClass.getVersion());
	}

	@Test
	public void expiryWhenTouchOnReadDocument() throws InterruptedException {
		String id = "simple-doc-with-update-expiry-for-read";
		DocumentWithTouchOnRead doc = new DocumentWithTouchOnRead(id);
		template.save(doc);
		Thread.sleep(1500);
		assertNotNull(template.findById(id, DocumentWithTouchOnRead.class));
		Thread.sleep(1500);
		assertNotNull(template.findById(id, DocumentWithTouchOnRead.class));
		Thread.sleep(3000);
		assertNull(template.findById(id, DocumentWithTouchOnRead.class));
	}

	/**
	 * A sample document with just an id and property.
	 */
	@Document
	static class SimplePerson {

		@Id
		private final String id;
		@Field
		private final String name;

		public SimplePerson(String id, String name) {
			this.id = id;
			this.name = name;
		}
	}

	/**
	 * A sample document that expires in 2 seconds.
	 */
	@Document(expiry = 2)
	static class DocumentWithExpiry {

		@Id
		private final String id;

		public DocumentWithExpiry(String id) {
			this.id = id;
		}
	}

	/**
	 * A sample document that expires in 2 seconds and touchOnRead set.
	 */
	@Document(expiry = 2, touchOnRead = true)
	static class DocumentWithTouchOnRead {

		@Id
		private final String id;

		public DocumentWithTouchOnRead(String id) {
			this.id = id;
		}
	}

	@Document
	static class ComplexPerson {

		@Id
		private final String id;
		@Field
		private final List<String> firstnames;
		@Field
		private final List<Integer> votes;

		@Field
		private final Map<String, Boolean> info1;
		@Field
		private final Map<String, Integer> info2;

		public ComplexPerson(String id, List<String> firstnames,
							 List<Integer> votes, Map<String, Boolean> info1,
							 Map<String, Integer> info2) {
			this.id = id;
			this.firstnames = firstnames;
			this.votes = votes;
			this.info1 = info1;
			this.info2 = info2;
		}

		List<String> getFirstnames() {
			return firstnames;
		}

		List<Integer> getVotes() {
			return votes;
		}

		Map<String, Boolean> getInfo1() {
			return info1;
		}

		Map<String, Integer> getInfo2() {
			return info2;
		}

		String getId() {
			return id;
		}
	}

	@Document
	static class SimpleWithLongAndInt {

		@Id
		private String id;

		private long longValue;
		private int intValue;

		SimpleWithLongAndInt(final String id, final long longValue, int intValue) {
			this.id = id;
			this.longValue = longValue;
			this.intValue = intValue;
		}

		String getId() {
			return id;
		}

		long getLongValue() {
			return longValue;
		}

		void setLongValue(final long value) {
			this.longValue = value;
		}

		public int getIntValue() {
			return intValue;
		}

		public void setIntValue(int intValue) {
			this.intValue = intValue;
		}
	}

	static class SimpleWithEnum {

		@Id
		private String id;

		private enum Type {
			BIG
		}

		private Type type;

		SimpleWithEnum(final String id, final Type type) {
			this.id = id;
			this.type = type;
		}

		String getId() {
			return id;
		}

		void setId(final String id) {
			this.id = id;
		}

		Type getType() {
			return type;
		}

		void setType(final Type type) {
			this.type = type;
		}
	}

	static class SimpleWithClass {

		@Id
		private String id;

		private Class<Integer> integerClass;

		private String value;

		SimpleWithClass(final String id, final Class<Integer> integerClass) {
			this.id = id;
			this.integerClass = integerClass;
		}

		String getId() {
			return id;
		}

		void setId(final String id) {
			this.id = id;
		}

		Class<Integer> getIntegerClass() {
			return integerClass;
		}

		void setIntegerClass(final Class<Integer> integerClass) {
			this.integerClass = integerClass;
		}

		String getValue() {
			return value;
		}

		void setValue(final String value) {
			this.value = value;
		}
	}

	static class VersionedClass {

		@Id
		private String id;

		@Version
		private long version;

		private String field;

		VersionedClass(String id, String field) {
			this.id = id;
			this.field = field;
		}

		public String getId() {
			return id;
		}

		public long getVersion() {
			return version;
		}

		public String getField() {
			return field;
		}

		public void setField(String field) {
			this.field = field;
		}
	}

	@Document
	static class FullFragment {

		@Id
		private String id;

		private long criteria;

		private String type;

		private String value;

		public FullFragment(String id, long criteria, String type, String value) {
			this.id = id;
			this.criteria = criteria;
			this.type = type;
			this.value = value;
		}

		public String getId() {
			return id;
		}

		public long getCriteria() {
			return criteria;
		}

		public String getType() {
			return type;
		}

		public String getValue() {
			return value;
		}

		public void setCriteria(long criteria) {
			this.criteria = criteria;
		}

		public void setType(String type) {
			this.type = type;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}

	static class Fragment {
		public String value;
	}
}
