package org.springframework.data.couchbase.repository.query;

import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

import java.util.Iterator;

import static org.springframework.data.couchbase.core.query.QueryCriteria.*;

public class N1qlQueryCreator extends AbstractQueryCreator<Query, QueryCriteria> {

	private final ParameterAccessor accessor;
	private final MappingContext<?, CouchbasePersistentProperty> context;

	public N1qlQueryCreator(PartTree tree, ParameterAccessor accessor,
													MappingContext<?, CouchbasePersistentProperty> context) {
		super(tree, accessor);
		this.accessor = accessor;
		this.context = context;
	}

	@Override
	protected QueryCriteria create(Part part, Iterator<Object> iterator) {
		PersistentPropertyPath<CouchbasePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		CouchbasePersistentProperty property = path.getLeafProperty();
		return from(part, property, where(path.toDotPath()), iterator);
	}

	@Override
	protected QueryCriteria and(Part part, QueryCriteria base, Iterator<Object> iterator) {
		if (base == null) {
			return create(part, iterator);
		}

		PersistentPropertyPath<CouchbasePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		CouchbasePersistentProperty property = path.getLeafProperty();

		return from(part, property, base.and(path.toDotPath()), iterator);
	}

	@Override
	protected QueryCriteria or(QueryCriteria base, QueryCriteria criteria) {
		//return new QueryCriteria().or(base, criteria);
		return new QueryCriteria();
	}

	@Override
	protected Query complete(QueryCriteria criteria, Sort sort) {
		return (criteria == null ? new Query() : new Query().addCriteria(criteria)).with(sort);
	}

	private QueryCriteria from(final Part part, final CouchbasePersistentProperty property, final QueryCriteria criteria,
														 final Iterator<Object> parameters) {

		final Part.Type type = part.getType();

		switch (type) {
			case AFTER:
			case GREATER_THAN:
				return criteria.gt(parameters.next());
			case GREATER_THAN_EQUAL:
				return criteria.gte(parameters.next());
			case SIMPLE_PROPERTY:
				return criteria.is(parameters.next());
			default:
				throw new IllegalArgumentException("Unsupported keyword!");
		}
	}

}
