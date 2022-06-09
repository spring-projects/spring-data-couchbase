/*
 * Copyright 2022 the original author or authors.
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
package org.springframework.data.couchbase.transaction;

import java.util.Objects;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.transactions.TransactionAttemptContext;
import com.couchbase.client.java.transactions.TransactionGetResult;
import com.couchbase.client.java.transactions.TransactionQueryOptions;
import com.couchbase.client.java.transactions.TransactionQueryResult;

/**
 * Provides methods to allow an application's transaction logic to read, mutate, insert and delete documents, as well
 * as
 * commit or rollback the transaction.
 * <p>
 * These methods are blocking/synchronous.  See {@link CoreTransactionAttemptContext} for the asynchronous version (which is
 * the
 * preferred option, and which this class largely simply wraps).
 */
public class SpringTransactionAttemptContext {
    private final TransactionAttemptContext internal;

    SpringTransactionAttemptContext(TransactionAttemptContext internal) {
        this.internal = Objects.requireNonNull(internal);
    }

    /**
     * Gets a document from the specified Couchbase <code>bucket</code> matching the specified <code>id</code>.  If
     * the document is not found, a <code>DocumentNotFoundException</code> is thrown.  If not caught inside the transaction logic, this
     * particular exception will cause the overall transaction to abort with a thrown <code>TransactionFailed</code>.
     *
     * @param collection the Couchbase collection the document exists on
     * @param id     the document's ID
     * @return a <code>TransactionGetResult</code> containing the document
     */
    public TransactionGetResult get(Collection collection, String id) {
        return internal.get(collection, id);
    }

    /**
     * Mutates the specified <code>doc</code> with new content.
     * <p>
     * The mutation is staged until the transaction is committed.  That is, any read of the document by any Couchbase
     * component will see the document's current value, rather than this staged or 'dirty' data.  If the attempt is
     * rolled back, the staged mutation will be removed.
     * <p>
     * This staged data effectively locks the document from other transactional writes until the attempt completes
     * (commits or rolls back).
     * <p>
     * If the mutation fails with a <code>CasMismatchException</code>, or any other exception, the transaction will
     * automatically
     * rollback this attempt, then retry.
     *
     * @param doc     the doc to be updated
     * @param content the content to replace the doc with.  This will normally be a {@link JsonObject}.
     * @return the doc, updated with its new CAS value.  For performance a copy is not created and the original doc
     * object is modified.
     */
    public TransactionGetResult replace(TransactionGetResult doc, Object content) {
        return internal.replace(doc, content);
    }

    /**
     * Inserts a new document into the specified Couchbase <code>collection</code>.
     * <p>
     * As with {@link #replace}, the insert is staged until the transaction is committed.  Due to technical limitations
     * it is not as possible to completely hide the staged data from the rest of the Couchbase platform, as an empty
     * document must be created.
     * <p>
     * This staged data effectively locks the document from other transactional writes until the attempt completes
     * (commits or rolls back).
     *
     * @param collection the Couchbase collection in which to insert the doc
     * @param id         the document's unique ID
     * @param content    the content to insert.  Generally this will be a
     *                   {@link com.couchbase.client.java.json.JsonObject}
     * @return the doc, updated with its new CAS value and ID, and converted to a <code>TransactionGetResult</code>
     */
    public TransactionGetResult insert(Collection collection, String id, Object content) {
        return internal.insert(collection, id, content);
    }

    /**
     * Removes the specified <code>doc</code>.
     * <p>
     * As with {@link #replace}, the remove is staged until the transaction is committed.  That is, the document will
     * continue to exist, and the rest of the Couchbase platform will continue to see it.
     * <p>
     * This staged data effectively locks the document from other transactional writes until the attempt completes
     * (commits or rolls back).
     * <p>
     * Note that a <code>remove(String id)</code> method is not possible, as it's necessary to check a provided
     * <code>TransactionGetResult</code> to determine if the document is involved in another transaction.
     *
     * @param doc the doc to be removed
     */
    public void remove(TransactionGetResult doc) {
        internal.remove(doc);
    }

    /**
     * Runs a N1QL query and returns the result.
     * <p>
     * All rows are buffered in-memory.
     * <p>
     * @throws CouchbaseException or an error derived from it on failure.  The application can choose to catch and ignore this error, and the
     *       transaction attempt is allowed to continue.  This differs from Key-Value operations, whose failure will
     *       cause the attempt to fail.
     */
    public TransactionQueryResult query(String statement,
                                        TransactionQueryOptions options) {
        return internal.query(statement, options);
    }

    /**
     * Calls query() with default options.
     */
    public TransactionQueryResult query(String statement) {
        return internal.query(statement);
    }

    /**
     * Runs a N1QL query and returns the result.
     * <p>
     * All rows are buffered in-memory.
     * <p>
     * This overload performs a 'scope-level query': that is, one in which a collection may be referenced by name in the
     * query statement, without needing to specify the full bucket.scope.collection syntax.
     * <p>
     * @throws CouchbaseException or an error derived from it on failure.  The application can choose to catch and ignore this error, and the
     *       transaction attempt is allowed to continue.  This differs from Key-Value operations, whose failure will
     *       cause the attempt to fail.
     */
    public TransactionQueryResult query(Scope scope,
                             String statement,
                             TransactionQueryOptions options) {
        return internal.query(scope, statement, options);
    }

    /**
     * Calls query() with default options.
     * <p>
     * This overload performs a 'scope-level query': that is, one in which a collection may be referenced by name in the
     * query statement, without needing to specify the full bucket.scope.collection syntax.
     */
    public TransactionQueryResult query(Scope scope, String statement) {
        return internal.query(scope, statement);
    }
}