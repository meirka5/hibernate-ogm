/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.dialect.spi;

/**
 * Container for the information needed to query a datastore.
 *
 * @author Davide D'Alto
 */
public interface QueryContext {

	/**
	 * A {@link TupleContext} keep track of the information needed to rebuild a single entity.
	 * <p>
	 * It can be {@code null} when the result of the query is not bound to a single entity.
	 *
	 * @return the tuple context associated to the entity the query will return, or {@code null} if the query does not return a single entity
	 */
	TupleContext getTupleContext();

	/**
	 * Some databases require information about the running transaction to execute a query, the
	 * {@link TransactionContext} will provide information related to the running transaction.
	 *
	 * @return a {@link TransactionContext} with the information related to the running transaction.
	 */
	TransactionContext getTransactionContext();
}
