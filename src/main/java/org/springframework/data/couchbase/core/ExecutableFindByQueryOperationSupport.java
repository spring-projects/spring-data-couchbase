package org.springframework.data.couchbase.core;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;
import org.springframework.data.couchbase.core.query.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ExecutableFindByQueryOperationSupport implements ExecutableFindByQueryOperation {

  private static final Query ALL_QUERY = new Query();

  private final CouchbaseTemplate template;

  public ExecutableFindByQueryOperationSupport(final CouchbaseTemplate template) {
    this.template = template;
  }

  @Override
  public <T> ExecutableFindByQuery<T> findByQuery(final Class<T> domainType) {
    return new ExecutableFindByQuerySupport<>(template, domainType, ALL_QUERY);
  }

  static class ExecutableFindByQuerySupport<T> implements ExecutableFindByQuery<T> {

    private final CouchbaseTemplate template;
    private final Class<T> domainType;
    private final Query query;

    ExecutableFindByQuerySupport(final CouchbaseTemplate template, final Class<T> domainType, final Query query) {
      this.template = template;
      this.domainType = domainType;
      this.query = query;
    }

    @Override
    public T oneValue() {
      return null;
    }

    @Override
    public T firstValue() {
      // TODO: add all the where clauses and _class
      String statement = assembleEntityQuery(1, false);
      QueryResult result = template.getCouchbaseClientFactory().getCluster().query(statement);
      List<T> decoded = rowsToEntities(result.rowsAsObject());
      if (decoded.isEmpty()) {
        return null;
      } else {
        return decoded.get(0);
      }
    }

    @Override
    public List<T> all() {
      // TODO: add all the where clauses and _class
      String statement = assembleEntityQuery(0, false);
      QueryResult result = template.getCouchbaseClientFactory().getCluster().query(statement);
      return rowsToEntities(result.rowsAsObject());
    }

    @Override
    public TerminatingFindByQuery<T> matching(final Query query) {
      return new ExecutableFindByQuerySupport<>(template, domainType, query);
    }

    private String assembleEntityQuery(final int limit, final boolean count) {
      String bucket = "`" + template.getBucketName() + "`";

      String project;
      if (count) {
        project = "count(*) as __count";
      } else {
        project = "meta().id as __id, meta().cas as __cas, " + bucket +".*";
      }

      String typeKey = template.getConverter().getTypeKey();
      String typeValue = template.support().getJavaNameForEntity(domainType);

      String where = " WHERE `" + typeKey + "` = \"" + typeValue + "\"";

      String statement = "SELECT " + project + " FROM " + bucket + where;
      if (limit > 0) {
        statement = statement + " LIMIT " + limit;
      }
      return statement;
    }

    private List<T> rowsToEntities(final List<JsonObject> input) {
      final List<T> converted = new ArrayList<>(input.size());
      for (JsonObject row : input) {
        String id = row.getString("__id");
        long cas = row.getLong("__cas");
        row.removeKey("__id");
        row.removeKey("__cas");
        converted.add(template.support().decodeEntity(id, row.toString(), cas, domainType));
      }
      return converted;
    }

    @Override
    public Stream<T> stream() {
      return all().stream();
    }

    @Override
    public long count() {
      String statement = assembleEntityQuery(0, false);
      QueryResult result = template.getCouchbaseClientFactory().getCluster().query(statement);
      return result.rowsAsObject().get(0).getLong("__count");
    }

    @Override
    public boolean exists() {
      return count() > 0;
    }

  }

}
