/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.util.parser.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.hibernate.LockOptions;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.dialect.TupleIterator;
import org.hibernate.ogm.loader.OgmLoader;
import org.hibernate.ogm.loader.OgmLoadingContext;
import org.hibernate.ogm.persister.OgmEntityPersister;

/**
 * Return each element of a {@link TupleIterator} as an entity or as the result of a projection.
 *
 * @author Davide D'Alto
 */
public class ObjectLoadingIterator implements Iterator<Object> {

	private final TupleIterator tupleIterator;
	private final Collection<String> projections;
	private final SessionImplementor session;
	private final Class<?> entityType;

	public ObjectLoadingIterator(SessionImplementor session, TupleIterator tupleIterator, Class<?> entityType, Collection<String> projections) {
		this.entityType = entityType;
		this.session = session;
		this.tupleIterator = tupleIterator;
		this.projections = projections;
	}

	@Override
	public boolean hasNext() {
		return tupleIterator.hasNext();
	}

	@Override
	public Object next() {
		Tuple next = tupleIterator.next();
		if ( projections.isEmpty() ) {
			return getAsManagedEntity( next );
		}
		else {
			return getAsProjection( next );
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException( "Not implemented yet" );
	}

	public void close() {
		tupleIterator.close();
	}

	private Object getAsManagedEntity(Tuple tuple) {
		OgmEntityPersister persister = (OgmEntityPersister) ( session.getFactory() ).getEntityPersister( entityType.getName() );
		OgmLoader loader = new OgmLoader( new OgmEntityPersister[] { persister } );
		OgmLoadingContext ogmLoadingContext = new OgmLoadingContext();
		ogmLoadingContext.setTuples( Arrays.asList( tuple ) );

		return loader.loadEntities( session, LockOptions.NONE, ogmLoadingContext ).iterator().next();
	}

	private Object[] getAsProjection(Tuple tuple) {
		Object[] projectionResult = new Object[projections.size()];
		int i = 0;

		for ( String column : projections ) {
			projectionResult[i] = tuple.get( column );
			i++;
		}

		return projectionResult;
	}

}
