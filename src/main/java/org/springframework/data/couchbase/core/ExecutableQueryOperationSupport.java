package org.springframework.data.couchbase.core;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ExecutableQueryOperationSupport implements ExecutableQueryOperation {

  private final CouchbaseTemplate template;

  public ExecutableQueryOperationSupport(final CouchbaseTemplate template) {
    this.template = template;
  }

  @Override
  public <T> ExecutableQuery<T> query(final Class<T> domainType) {
    return new ExecutableQuerySupport<>(template, domainType);
  }

  static class ExecutableQuerySupport<T> implements ExecutableQuery<T> {

    private final CouchbaseTemplate template;
    private final Class<T> domainType;

    ExecutableQuerySupport(final CouchbaseTemplate template, final Class<T> domainType) {
      this.template = template;
      this.domainType = domainType;
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
