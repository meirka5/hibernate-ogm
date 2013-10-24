/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2010-2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.hibernate.ogm.dialect.infinispan;

import static org.hibernate.ogm.datastore.spi.DefaultDatastoreNames.ASSOCIATION_STORE;
import static org.hibernate.ogm.datastore.spi.DefaultDatastoreNames.ENTITY_STORE;
import static org.hibernate.ogm.datastore.spi.DefaultDatastoreNames.IDENTIFIER_STORE;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.LockMode;
import org.hibernate.dialect.lock.LockingStrategy;
import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.loader.custom.CustomQuery;
import org.hibernate.ogm.datastore.impl.EmptyTupleSnapshot;
import org.hibernate.ogm.datastore.impl.MapHelpers;
import org.hibernate.ogm.datastore.infinispan.impl.HotRodDatastoreProvider;
import org.hibernate.ogm.datastore.map.impl.MapAssociationSnapshot;
import org.hibernate.ogm.datastore.spi.Association;
import org.hibernate.ogm.datastore.spi.AssociationContext;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.datastore.spi.TupleContext;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.EntityKeyMetadata;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.massindex.batchindexing.Consumer;
import org.hibernate.ogm.type.GridType;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.type.Type;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;

/**
 * @author Emmanuel Bernard
 */
@SuppressWarnings("unchecked")
public class HotRodDialect implements GridDialect {

	private final HotRodDatastoreProvider provider;

	public HotRodDialect(HotRodDatastoreProvider provider) {
		this.provider = provider;
	}

	/**
	 * Get a strategy instance which knows how to acquire a database-level lock of the specified mode for this dialect.
	 *
	 * @param lockable The persister for the entity to be locked.
	 * @param lockMode The type of lock to be acquired.
	 * @return The appropriate locking strategy.
	 * @since 3.2
	 */
	@Override
	public LockingStrategy getLockingStrategy(Lockable lockable, LockMode lockMode) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Tuple getTuple(EntityKey key, TupleContext tupleContext) {
		RemoteCache<EntityKey, Map<String, Object>> cache = provider.getCache( ENTITY_STORE );
		Map<String, Object> atomicMap = cache.get( key );
		if ( atomicMap == null ) {
			return null;
		}
		else {
			return new Tuple( new HotRodTupleSnapshot( atomicMap ) );
		}
	}

	@Override
	public Tuple createTuple(EntityKey key) {
		Map<String, Object> newMap = new ConcurrentHashMap<String, Object>();
		return new Tuple( new HotRodTupleSnapshot( newMap ) );
	}

	@Override
	public void updateTuple(Tuple tuple, EntityKey key) {
		Map<String, Object> atomicMap = ( (HotRodTupleSnapshot) tuple.getSnapshot() ).getAtomicMap();
		MapHelpers.applyTupleOpsOnMap( tuple, atomicMap );
		RemoteCache<EntityKey, Map<String, Object>> cache = provider.getCache( ENTITY_STORE );
		cache.put( key, atomicMap );
	}

	@Override
	public void removeTuple(EntityKey key) {
		RemoteCache<EntityKey, Map<String, Object>> cache = provider.getCache( ENTITY_STORE );
		cache.remove( key );
	}

	@Override
	public Association getAssociation(AssociationKey key, AssociationContext associationContext) {
		RemoteCache<AssociationKey, Map<RowKey, Map<String, Object>>> cache = provider.getCache( ASSOCIATION_STORE );
		Map<RowKey, Map<String, Object>> atomicMap = cache.get( key );
		return atomicMap == null ? null : new Association( new MapAssociationSnapshot( atomicMap ) );
	}

	@Override
	public Association createAssociation(AssociationKey key) {
		return new Association( new MapAssociationSnapshot( new ConcurrentHashMap<RowKey, Map<String, Object>>() ) );
	}

	@Override
	public void updateAssociation(Association association, AssociationKey key) {
		MapHelpers.updateAssociation( association, key );
		RemoteCache<AssociationKey, Map<RowKey, Map<String, Object>>> cache = provider.getCache( ASSOCIATION_STORE );
		Map<RowKey, Map<String, Object>> atomicMap = ( (MapAssociationSnapshot) association.getSnapshot() ).getUnderlyingMap();
		cache.put( key, atomicMap );
	}

	@Override
	public void removeAssociation(AssociationKey key) {
		RemoteCache<AssociationKey, Map<RowKey, Map<String, Object>>> cache = provider.getCache( ASSOCIATION_STORE );
		cache.remove( key );
	}

	@Override
	public Tuple createTupleAssociation(AssociationKey associationKey, RowKey rowKey) {
		return new Tuple( EmptyTupleSnapshot.SINGLETON );
	}

	@Override
	public void nextValue(RowKey key, IntegralDataTypeHolder value, int increment, int initialValue) {
		final RemoteCache<RowKey, Object> identifierCache = provider.getCache( IDENTIFIER_STORE );
		boolean done = false;
		do {
			// read value
			VersionedValue<Object> valueFromDb = identifierCache.getVersioned( key );
			if ( valueFromDb == null ) {
				// if not there, insert initial value
				value.initialize( initialValue );
				// TODO should we use GridTypes here?
				Long newValue = new Long( value.makeValue().longValue() );
				final boolean replaced = identifierCache.replaceWithVersion( key, newValue, valueFromDb.getVersion() );
			}
			else {
				// read the value from the table
				value.initialize( ( (Number) valueFromDb ).longValue() );
			}

			// update value
			final IntegralDataTypeHolder updateValue = value.copy();
			// increment value
			updateValue.add( increment );
			// TODO should we use GridTypes here?
			final Object newValueFromDb = updateValue.makeValue();
			done = identifierCache.replaceWithVersion( key, newValueFromDb, valueFromDb. );
		} while ( !done );
	}

	@Override
	public GridType overrideType(Type type) {
		return null;
	}

	@Override
	public Iterator<Tuple> executeBackendQuery(CustomQuery customQuery, EntityKeyMetadata[] metadatas) {
		throw new UnsupportedOperationException( "Native queries not supported for Infinispan" );
	}

	@Override
	@SuppressWarnings("unchecked")
	public void forEachTuple(Consumer consumer, EntityKeyMetadata... entityKeyMetadatas) {
		throw new UnsupportedOperationException( "It is not possible to scan the datastore using HotRod" );
	}
}
