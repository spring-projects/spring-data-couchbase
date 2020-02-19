package org.springframework.data.couchbase.core;

import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.query.ReactiveQueryResult;
import org.springframework.data.couchbase.core.query.Query;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Optional;

public class ExecutableRemoveByQueryOperationSupport implements ExecutableRemoveByQueryOperation {

  private static final Query ALL_QUERY = new Query();

  private final CouchbaseTemplate template;

  public ExecutableRemoveByQueryOperationSupport(final CouchbaseTemplate template) {
    this.template = template;
  }

  @Override
  public <T> ExecutableRemoveByQuery<T> removeByQuery(Class<T> domainType) {
    return new ExecutableRemoveByQuerySupport<>(template, domainType, ALL_QUERY, QueryScanConsistency.NOT_BOUNDED);
  }

  static class ExecutableRemoveByQuerySupport<T> implements ExecutableRemoveByQuery<T> {

    private final CouchbaseTemplate template;
    private final Class<T> domainType;
    private final Query query;
    private final TerminatingReactiveRemoveByQuerySupport<T> reactiveSupport;
    private final QueryScanConsistency scanConsistency;


    ExecutableRemoveByQuerySupport(final CouchbaseTemplate template, final Class<T> domainType, final Query query,
                                   final QueryScanConsistency scanConsistency) {
      this.template = template;
      this.domainType = domainType;
      this.query = query;
      this.reactiveSupport = new TerminatingReactiveRemoveByQuerySupport<>(template, domainType, query, scanConsistency);
      this.scanConsistency = scanConsistency;
    }

    @Override
    public List<RemoveResult> all() {
      return reactiveSupport.all().collectList().block();
    }

    @Override
    public TerminatingRemoveByQuery<T> matching(final Query query) {
      return new ExecutableRemoveByQuerySupport<>(template, domainType, query, scanConsistency);
    }

    @Override
    public RemoveByQueryWithQuery<T> consistentWith(final QueryScanConsistency scanConsistency) {
      return new ExecutableRemoveByQuerySupport<>(template, domainType, query, scanConsistency);
    }

    @Override
    public TerminatingReactiveRemoveByQuery<T> reactive() {
      return reactiveSupport;
    }
  }

  static class TerminatingReactiveRemoveByQuerySupport<T> implements TerminatingReactiveRemoveByQuery<T> {

    private final CouchbaseTemplate template;
    private final Class<T> domainType;
    private final Query query; // TODO
    private final QueryScanConsistency scanConsistency;

    TerminatingReactiveRemoveByQuerySupport(final CouchbaseTemplate template, final Class<T> domainType,
                                            final Query query, final QueryScanConsistency scanConsistency) {
      this.template = template;
      this.domainType = domainType;
      this.query = query;
      this.scanConsistency = scanConsistency;
    }

    @Override
    public Flux<RemoveResult> all() {
      return Flux.defer(() -> {
        String bucket = "`" + template.getBucketName() + "`";

        String typeKey = template.getConverter().getTypeKey();
        String typeValue = template.support().getJavaNameForEntity(domainType);
        String where = " WHERE `" + typeKey + "` = \"" + typeValue + "\"";

        String returning = " RETURNING meta().*";
        String statement = "DELETE FROM " + bucket + " " + where + returning;

        return template
          .getCouchbaseClientFactory()
          .getCluster()
          .reactive()
          .query(statement, buildQueryOptions())
          .onErrorMap(throwable -> {
            if (throwable instanceof RuntimeException) {
              return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
            } else {
              return throwable;
            }
          })
          .flatMapMany(ReactiveQueryResult::rowsAsObject)
          .map(row -> new RemoveResult(row.getString("id"), row.getLong("cas"), Optional.empty()));
      });
    }

    private QueryOptions buildQueryOptions() {
      final QueryOptions options = QueryOptions.queryOptions();
      if (scanConsistency != null) {
        options.scanConsistency(scanConsistency);
      }
      return options;
    }
  }

}
