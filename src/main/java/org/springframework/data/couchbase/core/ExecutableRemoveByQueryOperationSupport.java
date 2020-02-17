package org.springframework.data.couchbase.core;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.query.QueryResult;
import org.springframework.data.couchbase.core.query.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExecutableRemoveByQueryOperationSupport implements ExecutableRemoveByQueryOperation {

  private static final Query ALL_QUERY = new Query();

  private final CouchbaseTemplate template;

  public ExecutableRemoveByQueryOperationSupport(final CouchbaseTemplate template) {
    this.template = template;
  }

  @Override
  public <T> ExecutableRemoveByQuery<T> removeByQuery(Class<T> domainType) {
    return new ExecutableRemoveByQuerySupport<>(template, domainType, ALL_QUERY);
  }

  static class ExecutableRemoveByQuerySupport<T> implements ExecutableRemoveByQuery<T> {

    private final CouchbaseTemplate template;
    private final Class<T> domainType;
    private final Query query;

    ExecutableRemoveByQuerySupport(final CouchbaseTemplate template, final Class<T> domainType, final Query query) {
      this.template = template;
      this.domainType = domainType;
      this.query = query;
    }

    @Override
    public List<RemoveResult> all() {
      String bucket = "`" + template.getBucketName() + "`";

      String typeKey = template.getConverter().getTypeKey();
      String typeValue = template.support().getJavaNameForEntity(domainType);
      String where = " WHERE `" + typeKey + "` = \"" + typeValue + "\"";

      String returning = " RETURNING meta().*";
      String statement = "DELETE FROM " + bucket + " " + where + returning;

      QueryResult result = template.getCouchbaseClientFactory().getCluster().query(statement);
      List<RemoveResult> toReturn = new ArrayList<>();
      for (JsonObject row : result.rowsAsObject()) {
        toReturn.add(new RemoveResult(row.getString("id"), row.getLong("cas"), Optional.empty()));
      }
      return toReturn;
    }

    @Override
    public TerminatingRemoveByQuery<T> matching(final Query query) {
      return new ExecutableRemoveByQuerySupport<>(template, domainType, query);
    }

  }

}
