/*
 * Copyright 2018 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.convert.join;

import static org.springframework.data.couchbase.core.support.TemplateUtils.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;

import com.couchbase.client.java.query.N1qlQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.query.FetchType;
import org.springframework.data.couchbase.core.query.HashSide;
import org.springframework.data.couchbase.core.query.N1qlJoin;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

/**
 * N1qlJoinResolver resolves by converting the join definition to query statement
 * and executing using CouchbaseTemplate
 *
 * @author Subhashni Balakrishnan
 */
public class N1qlJoinResolver {
    private static final Logger LOGGER = LoggerFactory.getLogger(N1qlJoinResolver.class);

    public static String buildQuery(CouchbaseTemplate template, N1qlJoinResolverParameters parameters) {
        String joinType = "JOIN"; //TODO: check if left join has any use cases
        String selectEntity = "SELECT META(rks).id AS " + SELECT_ID +
                ", META(rks).cas AS " + SELECT_CAS + ", (rks).* ";

        StringBuilder useLKSBuilder = new StringBuilder();
        if (parameters.getJoinDefinition().index().length() > 0) {
            useLKSBuilder.append("INDEX(" + parameters.getJoinDefinition().index() + ")");
        }
        String useLKS = useLKSBuilder.length() > 0 ? "USE " + useLKSBuilder.toString() + " " : "";

        String from = "FROM `" + template.getCouchbaseBucket().name() + "` lks " + useLKS + joinType + " " + template.getCouchbaseBucket().name() + " rks";
        String onLks = "lks." + template.getConverter().getTypeKey() + " = \""+ parameters.getEntityTypeInfo().getType().getName() + "\"";
        String onRks = "rks." + template.getConverter().getTypeKey() + " = \"" + parameters.getAssociatedEntityTypeInfo().getType().getName() + "\"";


        StringBuilder useRKSBuilder = new StringBuilder();
        if (parameters.getJoinDefinition().rightIndex().length() > 0) {
            useRKSBuilder.append("INDEX(" + parameters.getJoinDefinition().rightIndex() + ")");
        }
        if (!parameters.getJoinDefinition().hashside().equals(HashSide.NONE)) {
            if (useRKSBuilder.length() > 0) useRKSBuilder.append(" ");
            useRKSBuilder.append("HASH(" + parameters.getJoinDefinition().hashside().getValue() +")");
        }
        if (parameters.getJoinDefinition().keys().length > 0) {
            if (useRKSBuilder.length() > 0) useRKSBuilder.append(" ");
            useRKSBuilder.append("KEYS [");
            String[] keys = parameters.getJoinDefinition().keys();

            for(int i=0; i < keys.length;i++) {
                if(i != 0) useRKSBuilder.append(",");
                useRKSBuilder.append("\"" + keys[i] +"\"");
            }
            useRKSBuilder.append("]");
        }

        String on = "ON " + parameters.getJoinDefinition().on().concat(" AND " + onLks).concat(" AND " + onRks);

        String where = "WHERE META(lks).id=\"" + parameters.getLksId() + "\"";
        where += ((parameters.getJoinDefinition().where().length() > 0) ? " AND " + parameters.getJoinDefinition().where() : "");

        StringBuilder statementSb = new StringBuilder();
        statementSb.append(selectEntity);
        statementSb.append(" " + from);
        statementSb.append((useRKSBuilder.length() > 0? " USE "+ useRKSBuilder.toString() : ""));
        statementSb.append(" " + on);
        statementSb.append(" " + where);
        return statementSb.toString();
    }

    public static <R> List<R> doResolve(CouchbaseTemplate template,
                                        N1qlJoinResolverParameters parameters,
                                        Class<R> associatedEntityClass) {
        String statement = buildQuery(template, parameters);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Join query executed " + statement);
        }

        N1qlQuery query = N1qlQuery.simple(statement);
        return template.findByN1QL(query, associatedEntityClass);
    }

    public static boolean isLazyJoin(N1qlJoin joinDefinition) {
        return joinDefinition.fetchType().equals(FetchType.LAZY);
    }

    static public class N1qlJoinProxy implements InvocationHandler {
        private final CouchbaseTemplate template;
        private final N1qlJoinResolverParameters params;
        private List<?> resolved = null;

        public N1qlJoinProxy(CouchbaseTemplate template, N1qlJoinResolverParameters params) {
            this.template = template;
            this.params = params;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if(this.resolved == null) {
                this.resolved = doResolve(this.template, this.params, this.params.associatedEntityTypeInfo.getType());
            }
            return method.invoke(this.resolved, args);
        }
    }

    static public class N1qlJoinResolverParameters {
        private N1qlJoin joinDefinition;
        private String lksId;
        private TypeInformation<?> entityTypeInfo;
        private TypeInformation<?> associatedEntityTypeInfo;

        public N1qlJoinResolverParameters(N1qlJoin joinDefinition,
                                          String lksId,
                                          TypeInformation<?> entityTypeInfo,
                                          TypeInformation<?> associatedEntityTypeInfo) {
            Assert.notNull(joinDefinition, "The join definition is required");
            Assert.notNull(entityTypeInfo, "The entity type information is required");
            Assert.notNull(associatedEntityTypeInfo, "The associated entity type information is required");

            this.joinDefinition = joinDefinition;
            this.lksId = lksId;
            this.entityTypeInfo = entityTypeInfo;
            this.associatedEntityTypeInfo = associatedEntityTypeInfo;
        }

        public N1qlJoin getJoinDefinition() {
            return joinDefinition;
        }

        public String getLksId() {
            return lksId;
        }

        public TypeInformation getEntityTypeInfo() {
            return entityTypeInfo;
        }

        public TypeInformation getAssociatedEntityTypeInfo() {
            return associatedEntityTypeInfo;
        }
    }
}