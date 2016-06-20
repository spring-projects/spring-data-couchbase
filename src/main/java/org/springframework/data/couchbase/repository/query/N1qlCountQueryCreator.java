package org.springframework.data.couchbase.repository.query;

import java.util.Iterator;

import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.PartTree;

import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.path.LimitPath;
import com.couchbase.client.java.query.dsl.path.WherePath;

public class N1qlCountQueryCreator extends N1qlQueryCreator {

    public N1qlCountQueryCreator(PartTree tree, ParameterAccessor parameters, WherePath selectFrom,
            CouchbaseConverter converter, CouchbaseQueryMethod queryMethod) {
        super(tree, new CountParameterAccessor(parameters), selectFrom, converter, queryMethod);
    }
    
    @Override
    protected LimitPath complete(Expression criteria, Sort sort) {
        // Sorting is not allowed on aggregate count queries.
        return super.complete(criteria, null);
    }

    private static class CountParameterAccessor implements ParameterAccessor {

        private ParameterAccessor delegate;

        public CountParameterAccessor(ParameterAccessor delegate) {
            this.delegate = delegate;
        }

        public Pageable getPageable() {
            return delegate.getPageable() != null ? new CountPageable(delegate.getPageable()) : null;
        }

        public Sort getSort() {
            return null;
        }

        public Class<?> getDynamicProjection() {
            return delegate.getDynamicProjection();
        }

        public Object getBindableValue(int index) {
            return delegate.getBindableValue(index);
        }

        public boolean hasBindableNullValue() {
            return delegate.hasBindableNullValue();
        }

        public Iterator<Object> iterator() {
            return delegate.iterator();
        }
        
    }
    
    private static class CountPageable implements Pageable {
        
        private Pageable delegate;

        public CountPageable(Pageable delegate) {
            this.delegate = delegate;
        }

        public int getPageNumber() {
            return delegate.getPageNumber();
        }

        public int getPageSize() {
            return delegate.getPageSize();
        }

        public int getOffset() {
            return delegate.getOffset();
        }

        public Sort getSort() {
            return null;
        }

        public Pageable next() {
            return delegate.next();
        }

        public Pageable previousOrFirst() {
            return delegate.previousOrFirst();
        }

        public Pageable first() {
            return delegate.first();
        }

        public boolean hasPrevious() {
            return delegate.hasPrevious();
        }
        
    }

}
