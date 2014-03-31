/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.query.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.Query;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.engine.query.spi.ParameterMetadata;
import org.hibernate.internal.AbstractQueryImpl;
import org.hibernate.ogm.datastore.mongodb.impl.MongoDBDatastoreProvider;
import org.hibernate.ogm.datastore.mongodb.impl.MongoDBResultTupleIterable;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.grid.EntityKeyMetadata;
import org.hibernate.ogm.hibernatecore.impl.OgmSession;
import org.hibernate.ogm.persister.OgmEntityPersister;
import org.hibernate.ogm.util.parser.impl.ObjectLoadingIterator;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * Query implementation based on on MongoDB.
 *
 * @author Gunnar Morling
 */
public class MongoDBQueryImpl extends AbstractQueryImpl {

	private final DBObject query;
	private final MongoDBDatastoreProvider provider;
	private final Class<?> entityType;
	private final DBObject projections;

	public MongoDBQueryImpl(Class<?> entityType, DBObject query, DBObject projections, OgmSession session) {
		super( query.toString(), null, session.getDelegate(), new ParameterMetadata( null, null ) );
		this.query = query;
		this.entityType = entityType;
		this.projections = projections;
		this.provider = (MongoDBDatastoreProvider) session.getSessionFactory().getServiceRegistry().getService( DatastoreProvider.class );
	}

	@Override
	public ObjectLoadingIterator iterate() throws HibernateException {
		MongoDBResultTupleIterable resultsCursor = getResultsCursor();
		return new ObjectLoadingIterator( session, resultsCursor.iterator(), entityType, projections.keySet() );
	}

	@Override
	public ScrollableResults scroll() throws HibernateException {
		throw new UnsupportedOperationException( "Not implemented yet" );
	}

	@Override
	public ScrollableResults scroll(ScrollMode scrollMode) throws HibernateException {
		throw new UnsupportedOperationException( "Not implemented yet" );
	}

	@Override
	public List<?> list() throws HibernateException {
		ObjectLoadingIterator results = iterate();
		try {
			List<Object> result = new ArrayList<Object>();
			while ( results.hasNext() ) {
				result.add( results.next() );
			}
			return result;
		}
		finally {
			results.close();
		}
	}

	private boolean isProjection() {
		return !projections.keySet().isEmpty();
	}

	@Override
	public int executeUpdate() throws HibernateException {
		throw new UnsupportedOperationException( "Not implemented yet" );
	}

	@Override
	public Query setLockOptions(LockOptions lockOptions) {
		throw new UnsupportedOperationException( "Not implemented yet" );
	}

	@Override
	public Query setLockMode(String alias, LockMode lockMode) {
		throw new UnsupportedOperationException( "Not implemented yet" );
	}

	@Override
	public LockOptions getLockOptions() {
		throw new UnsupportedOperationException( "Not implemented yet" );
	}

	private MongoDBResultTupleIterable getResultsCursor() {
		EntityKeyMetadata keyMetaData = getKeyMetaData( entityType );
		DBCollection collection = provider.getDatabase().getCollection( keyMetaData.getTable() );

		DBCursor cursor = isProjection() ? collection.find( query, projections ) : collection.find( query );

		return new MongoDBResultTupleIterable( cursor, keyMetaData );
	}

	private EntityKeyMetadata getKeyMetaData(Class<?> entityType) {
		OgmEntityPersister persister = (OgmEntityPersister) ( session.getFactory() ).getEntityPersister( entityType.getName() );
		return new EntityKeyMetadata( persister.getTableName(), persister.getRootTableIdentifierColumnNames() );
	}

}
