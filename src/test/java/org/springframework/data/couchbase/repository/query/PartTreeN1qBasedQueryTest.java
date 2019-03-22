/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.data.couchbase.repository.query;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.couchbase.client.java.document.json.JsonObject;
import org.junit.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.couchbase.core.Beer;
import org.springframework.data.couchbase.core.BeerDTO;
import org.springframework.data.couchbase.core.BeerProjection;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.core.EntityMetadata;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ResultProcessor;

import com.couchbase.client.java.CouchbaseBucket;
import com.couchbase.client.java.query.Statement;

/**
 * @author Mark Paluch
 */
public class PartTreeN1qBasedQueryTest {

	@Test
	public void testGetCountExcludesStaticSortClause() throws Exception {

		PageRequest pr = new PageRequest(0, 10);

		CouchbaseOperations couchbaseOperations = mock(CouchbaseOperations.class);
		CouchbaseBucket couchbaseBucket = mock(CouchbaseBucket.class);
		CouchbaseConverter couchbaseConverter = mock(CouchbaseConverter.class);
		MappingContext mappingContext = mock(MappingContext.class);
		PersistentPropertyPath persistentPropertyPath = mock(PersistentPropertyPath.class);
		CouchbasePersistentProperty leafProperty = mock(CouchbasePersistentProperty.class);
		EntityMetadata entityInformation = mock(EntityMetadata.class);
		ParameterAccessor accessor = mock(ParameterAccessor.class);
		ProjectionFactory factory = mock(ProjectionFactory.class);

		Method method = TestRepository.class.getMethod("findByNameOrderByName", String.class, Pageable.class);
		RepositoryMetadata metadata = new DefaultRepositoryMetadata(TestRepository.class);

		CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, factory, mappingContext);

		when(accessor.getSort()).thenReturn(Sort.unsorted());
		when(accessor.getPageable()).thenReturn(Pageable.unpaged());
		when(entityInformation.getJavaType()).thenReturn(Beer.class);
		when(couchbaseOperations.getCouchbaseBucket()).thenReturn(couchbaseBucket);
		when(couchbaseBucket.name()).thenReturn("default");
		when(couchbaseOperations.getConverter()).thenReturn(couchbaseConverter);
		when(couchbaseConverter.getMappingContext()).thenReturn(mappingContext);
		when(mappingContext.getPersistentPropertyPath(isA(PropertyPath.class))).thenReturn(persistentPropertyPath);
		when(persistentPropertyPath.toDotPath(isA(Converter.class))).thenReturn("name");
		when(persistentPropertyPath.getLeafProperty()).thenReturn(leafProperty);
		when(leafProperty.getType()).thenReturn((Class) String.class);
		when(accessor.iterator()).thenReturn(Arrays.asList((Object) "value", pr).iterator());
		when(couchbaseConverter.getTypeKey()).thenReturn("_class");
		when(couchbaseConverter.convertForWriteIfNeeded(eq("value"))).thenReturn("value");
		when(accessor.getPageable()).thenReturn(pr);

		PartTreeN1qlBasedQuery query = new PartTreeN1qlBasedQuery(queryMethod, couchbaseOperations);
		Statement statement = query.getCount(accessor, new Object[] { "value", pr });

		assertEquals("SELECT COUNT(*) AS count FROM `default` WHERE (name = $1) "
				+ "AND `_class` = \"org.springframework.data.couchbase.core.Beer\"", statement.toString());

	}

	@Test
	public void testGetCountExcludesDynamicSortClause() throws Exception {

		Sort sort = new Sort(Direction.ASC, "name");
		PageRequest pr = new PageRequest(0, 10, sort);

		CouchbaseOperations couchbaseOperations = mock(CouchbaseOperations.class);
		CouchbaseBucket couchbaseBucket = mock(CouchbaseBucket.class);
		CouchbaseConverter couchbaseConverter = mock(CouchbaseConverter.class);
		MappingContext mappingContext = mock(MappingContext.class);
		PersistentPropertyPath persistentPropertyPath = mock(PersistentPropertyPath.class);
		CouchbasePersistentProperty leafProperty = mock(CouchbasePersistentProperty.class);
		EntityMetadata entityInformation = mock(EntityMetadata.class);
		ParameterAccessor accessor = mock(ParameterAccessor.class);
		ProjectionFactory factory = mock(ProjectionFactory.class);

		Method method = TestRepository.class.getMethod("findByName", String.class, Pageable.class);
		RepositoryMetadata metadata = new DefaultRepositoryMetadata(TestRepository.class);

		CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, factory, mappingContext);

		when(accessor.getPageable()).thenReturn(Pageable.unpaged());
		when(entityInformation.getJavaType()).thenReturn(Beer.class);
		when(couchbaseOperations.getCouchbaseBucket()).thenReturn(couchbaseBucket);
		when(couchbaseBucket.name()).thenReturn("default");
		when(couchbaseOperations.getConverter()).thenReturn(couchbaseConverter);
		when(couchbaseConverter.getMappingContext()).thenReturn(mappingContext);
		when(mappingContext.getPersistentPropertyPath(isA(PropertyPath.class))).thenReturn(persistentPropertyPath);
		when(persistentPropertyPath.toDotPath(isA(Converter.class))).thenReturn("name");
		when(persistentPropertyPath.getLeafProperty()).thenReturn(leafProperty);
		when(leafProperty.getType()).thenReturn((Class) String.class);
		when(accessor.iterator()).thenReturn(Arrays.asList((Object) "value", pr).iterator());
		when(accessor.getSort()).thenReturn(sort);
		when(couchbaseConverter.getTypeKey()).thenReturn("_class");
		when(couchbaseConverter.convertForWriteIfNeeded(eq("value"))).thenReturn("value");
		when(accessor.getPageable()).thenReturn(pr);

		PartTreeN1qlBasedQuery query = new PartTreeN1qlBasedQuery(queryMethod, couchbaseOperations);
		Statement statement = query.getCount(accessor, new Object[] { "value", pr });

		assertEquals("SELECT COUNT(*) AS count FROM `default` WHERE (name = $1) "
				+ "AND `_class` = \"org.springframework.data.couchbase.core.Beer\"", statement.toString());

	}

	@Test
	public void testProjectionInterface() throws Exception {

		CouchbaseOperations couchbaseOperations = mock(CouchbaseOperations.class);
		CouchbaseBucket couchbaseBucket = mock(CouchbaseBucket.class);
		CouchbaseConverter couchbaseConverter = mock(CouchbaseConverter.class);
		EntityMetadata entityInformation = mock(EntityMetadata.class);
		ParameterAccessor accessor = mock(ParameterAccessor.class);

		ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		factory.createProjection(BeerProjection.class);
		MappingContext mappingContext = new CouchbaseMappingContext();
		RepositoryMetadata metadata = new DefaultRepositoryMetadata(TestRepository.class);
		Method method = TestRepository.class.getMethod("findAllProjectedBy");
		CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, factory, mappingContext);

		when(accessor.getDynamicProjection()).thenReturn(Optional.of(BeerProjection.class));
		when(entityInformation.getJavaType()).thenReturn(Beer.class);
		when(couchbaseOperations.getCouchbaseBucket()).thenReturn(couchbaseBucket);
		when(couchbaseBucket.name()).thenReturn("B");
		when(couchbaseOperations.getConverter()).thenReturn(couchbaseConverter);
		when(couchbaseOperations.getConverter().getMappingContext()).thenReturn(mappingContext);
		when(couchbaseConverter.getTypeKey()).thenReturn("_class");
		when(couchbaseConverter.convertForWriteIfNeeded(eq("value"))).thenReturn("value");

		ResultProcessor processor = queryMethod.getResultProcessor().withDynamicProjection(accessor);

		PartTreeN1qlBasedQuery query = new PartTreeN1qlBasedQuery(queryMethod, couchbaseOperations);
		Statement statement = query.getStatement(accessor, null, processor.getReturnedType());

		assertEquals("SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS, `B`.`desc` FROM `B` WHERE "
				+ "`_class` = \"org.springframework.data.couchbase.core.Beer\"", statement.toString());

	}

	@Test
	public void testProjectionDTO() throws Exception {
		CouchbaseOperations couchbaseOperations = mock(CouchbaseOperations.class);
		CouchbaseBucket couchbaseBucket = mock(CouchbaseBucket.class);
		CouchbaseConverter couchbaseConverter = mock(CouchbaseConverter.class);
		EntityMetadata entityInformation = mock(EntityMetadata.class);
		ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		ParameterAccessor accessor = mock(ParameterAccessor.class);

		RepositoryMetadata metadata = new DefaultRepositoryMetadata(TestRepository.class);
		Method method = TestRepository.class.getMethod("findAllDtoedBy");
		MappingContext mappingContext = new CouchbaseMappingContext();
		CouchbaseQueryMethod queryMethod = new CouchbaseQueryMethod(method, metadata, factory, mappingContext);

		when(accessor.getDynamicProjection()).thenReturn(Optional.of(BeerDTO.class));
		when(entityInformation.getJavaType()).thenReturn(Beer.class);
		when(couchbaseOperations.getCouchbaseBucket()).thenReturn(couchbaseBucket);
		when(couchbaseBucket.name()).thenReturn("B");
		when(couchbaseOperations.getConverter()).thenReturn(couchbaseConverter);
		when(couchbaseOperations.getConverter().getMappingContext()).thenReturn(mappingContext);
		when(couchbaseConverter.getTypeKey()).thenReturn("_class");

		ResultProcessor processor = queryMethod.getResultProcessor().withDynamicProjection(accessor);

		PartTreeN1qlBasedQuery query = new PartTreeN1qlBasedQuery(queryMethod, couchbaseOperations);
		Statement statement = query.getStatement(accessor, null, processor.getReturnedType());

		assertEquals("SELECT META(`B`).id AS _ID, META(`B`).cas AS _CAS, `B`.`name`, `B`.`desc` FROM `B` "
				+ "WHERE `_class` = \"org.springframework.data.couchbase.core.Beer\"", statement.toString());

	}

	public static interface TestRepository extends CrudRepository<Beer, String> {

		Page<Beer> findByNameOrderByName(String name, Pageable pageRequest);

		Page<Beer> findByName(String name, Pageable pageRequest);

		Collection<BeerProjection> findAllProjectedBy();

		List<BeerDTO> findAllDtoedBy();

	}
}
