package org.springframework.data.couchbase.repository.extending.method;

import java.util.List;

import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.query.N1QLExpression;
import org.springframework.data.couchbase.core.query.N1QLQuery;
import org.springframework.data.couchbase.repository.Item;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.query.CouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.query.CountFragment;
import org.springframework.data.couchbase.repository.query.support.N1qlUtils;
import org.springframework.data.couchbase.repository.support.MappingCouchbaseEntityInformation;

public class MyRepositoryImpl implements MyRepositoryCustom {

  @Autowired
  RepositoryOperationsMapping templateProvider;

  @Override
  public long customCountItems() {
    CouchbaseOperations template = templateProvider.resolve(MyRepository.class, Item.class);

    CouchbasePersistentEntity<Object> itemPersistenceEntity = (CouchbasePersistentEntity<Object>)
        template.getConverter()
            .getMappingContext()
            .getRequiredPersistentEntity(MyItem.class);

    CouchbaseEntityInformation<? extends Object, String> itemEntityInformation =
        new MappingCouchbaseEntityInformation<Object, String>(itemPersistenceEntity);

    N1QLExpression countStatement = N1qlUtils.createCountQueryForEntity(
        template.getCouchbaseBucket().name(),
        template.getConverter(),
        itemEntityInformation);

    QueryScanConsistency consistency = template.getDefaultConsistency().n1qlConsistency();
    N1QLQuery query = new N1QLQuery(countStatement, QueryOptions.queryOptions().scanConsistency(consistency));

    List<CountFragment> countFragments = template.findByN1QLProjection(query, CountFragment.class);

    if (countFragments == null || countFragments.isEmpty()) {
      return 0L;
    } else {
      return countFragments.get(0).count * -1L;
    }
  }

  public long count() {
    return 100;
  }
}
