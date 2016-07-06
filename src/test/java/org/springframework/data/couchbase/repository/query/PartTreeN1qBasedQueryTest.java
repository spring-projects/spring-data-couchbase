package org.springframework.data.couchbase.repository.query;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.junit.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.couchbase.core.Beer;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.core.EntityMetadata;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.ParameterAccessor;

import com.couchbase.client.java.CouchbaseBucket;
import com.couchbase.client.java.query.Statement;

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

		PartTreeN1qlBasedQuery query = new PartTreeN1qlBasedQuery(queryMethod, couchbaseOperations);
		Statement statement = query.getCount(accessor, new Object[] { "value", pr });

		assertEquals("SELECT COUNT(*) AS count FROM `default` WHERE name = \"value\" "
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

		PartTreeN1qlBasedQuery query = new PartTreeN1qlBasedQuery(queryMethod, couchbaseOperations);
		Statement statement = query.getCount(accessor, new Object[] { "value", pr });

		assertEquals("SELECT COUNT(*) AS count FROM `default` WHERE name = \"value\" "
				+ "AND `_class` = \"org.springframework.data.couchbase.core.Beer\"", statement.toString());

	}

	public static interface TestRepository extends CrudRepository<Beer, String> {

		Page<Beer> findByNameOrderByName(String name, Pageable pageRequest);

		Page<Beer> findByName(String name, Pageable pageRequest);

	}

}
