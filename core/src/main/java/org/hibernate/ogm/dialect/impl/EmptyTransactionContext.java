/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.dialect.impl;

import org.hibernate.ogm.dialect.spi.TransactionContext;
import org.hibernate.ogm.util.impl.Log;
import org.hibernate.ogm.util.impl.LoggerFactory;

/**
 * @author Davide D'Alto
 */
public class EmptyTransactionContext implements TransactionContext {

	private static final Log LOG = LoggerFactory.make();

	public static TransactionContext INSTANCE = new EmptyTransactionContext();

	private EmptyTransactionContext() {
	}

	@Override
	public Object getTransactionId() {
		throw LOG.transactionIdIsNull();
	}

}
