/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.parser.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.Query;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.engine.query.spi.ParameterMetadata;
import org.hibernate.internal.AbstractQueryImpl;
import org.hibernate.ogm.datastore.neo4j.dialect.impl.MapsTupleIterator;
import org.hibernate.ogm.datastore.neo4j.dialect.impl.NodesTupleIterator;
import org.hibernate.ogm.datastore.neo4j.impl.Neo4jDatastoreProvider;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.dialect.TupleIterator;
import org.hibernate.ogm.exception.NotSupportedException;
import org.hibernate.ogm.hibernatecore.impl.OgmSession;
import org.hibernate.ogm.util.parser.impl.ObjectLoadingIterator;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class Neo4jQuery extends AbstractQueryImpl {

	private final Class<?> entityType;
	private final List<String> projections;
	private final ExecutionResult executeResult;

	public Neo4jQuery(Class<?> entityType, String query, List<String> projections, OgmSession session) {
		super( query, null, session, new ParameterMetadata( null, null ) );
		Neo4jDatastoreProvider provider = neo4jProvider( session );
		ExecutionEngine engine = new ExecutionEngine( provider.getDataBase() );
		this.entityType = entityType;
		this.projections = projections;
		this.executeResult = engine.execute( getQueryString() );
	}

	private Neo4jDatastoreProvider neo4jProvider(OgmSession session) {
		return (Neo4jDatastoreProvider) session.getSessionFactory().getServiceRegistry().getService( DatastoreProvider.class );
	}

	@Override
	public Query setLockOptions(LockOptions lockOptions) {
		return null;
	}

	@Override
	public Query setLockMode(String alias, LockMode lockMode) {
		return null;
	}

	@Override
	public Iterator<?> iterate() {
		TupleIterator iterator = null;
		if ( projections.isEmpty() ) {
			iterator = new NodesTupleIterator( executeResult );
		}
		else {
			iterator = new MapsTupleIterator( executeResult );
		}
		return new ObjectLoadingIterator( session, iterator, entityType, projections );
	}

	@Override
	public ScrollableResults scroll() {
		return null;
	}

	@Override
	public ScrollableResults scroll(ScrollMode scrollMode) {
		return null;
	}

	@Override
	public List<?> list() {
		ObjectLoadingIterator iterator = (ObjectLoadingIterator) iterate();
		try {
			List<Object> result = new ArrayList<Object>();
			while ( iterator.hasNext() ) {
				Object next = iterator.next();
				result.add( next );
			}
			return result;
		}
		finally {
			iterator.close();
		}
	}

	@Override
	public int executeUpdate() {
		throw new NotSupportedException( "OGM", "This operation is not implemented yet" );
	}

	@Override
	public LockOptions getLockOptions() {
		return null;
	}

}
