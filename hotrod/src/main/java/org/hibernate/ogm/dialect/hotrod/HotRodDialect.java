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
package org.hibernate.ogm.dialect.hotrod;

import static org.hibernate.ogm.datastore.spi.DefaultDatastoreNames.ASSOCIATION_STORE;
import static org.hibernate.ogm.datastore.spi.DefaultDatastoreNames.ENTITY_STORE;
import static org.hibernate.ogm.datastore.spi.DefaultDatastoreNames.IDENTIFIER_STORE;

import java.util.Iterator;
import java.util.Map;

import org.hibernate.LockMode;
import org.hibernate.dialect.lock.LockingStrategy;
import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.loader.custom.CustomQuery;
import org.hibernate.ogm.datastore.hotrod.impl.HotRodDatastoreProvider;
import org.hibernate.ogm.datastore.impl.EmptyTupleSnapshot;
import org.hibernate.ogm.datastore.impl.MapHelpers;
import org.hibernate.ogm.datastore.map.impl.MapAssociationSnapshot;
import org.hibernate.ogm.datastore.spi.Association;
import org.hibernate.ogm.datastore.spi.AssociationContext;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.datastore.spi.TupleContext;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.dialect.hotrod.atomic.HotRodAtomicMapLookup;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.EntityKeyMetadata;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.massindex.batchindexing.Consumer;
import org.hibernate.ogm.type.GridType;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.type.Type;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
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
		Map<String, Object> atomicMap = HotRodAtomicMapLookup.getFineGrainedAtomicMap( provider.getRemoteCacheManager(), cache, key, false );
		if ( atomicMap == null ) {
			return null;
		}
		else {
			return new Tuple( new HotRodTupleSnapshot( atomicMap ) );
		}
	}

	@Override
	public Tuple createTuple(EntityKey key) {
		RemoteCache<EntityKey, Map<String, Object>> cache = provider.getCache( ENTITY_STORE );
		RemoteCacheManager cacheManager = provider.getRemoteCacheManager();
		Map<String, Object> atomicMap = HotRodAtomicMapLookup.getFineGrainedAtomicMap( cacheManager, cache, key, true );
		return new Tuple( new HotRodTupleSnapshot( atomicMap ) );
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
		HotRodAtomicMapLookup.removeAtomicMap( cache, key );
	}

	@Override
	public Association getAssociation(AssociationKey key, AssociationContext associationContext) {
		RemoteCache<AssociationKey, Map<RowKey, Map<String, Object>>> cache = provider.getCache( ASSOCIATION_STORE );
		Map<RowKey, Map<String, Object>> atomicMap = HotRodAtomicMapLookup.getFineGrainedAtomicMap( provider.getRemoteCacheManager(), cache, key, false );
		return atomicMap == null ? null : new Association( new MapAssociationSnapshot( atomicMap ) );
	}

	@Override
	public Association createAssociation(AssociationKey key, AssociationContext associationContext) {
		// We don't verify that it does not yet exist assuming that this has been done before by the calling code
		RemoteCache<AssociationKey, Map<RowKey, Map<String, Object>>> cache = provider.getCache( ASSOCIATION_STORE );
		Map<RowKey, Map<String, Object>> atomicMap = HotRodAtomicMapLookup.getFineGrainedAtomicMap( provider.getRemoteCacheManager(), cache, key, true );
		return new Association( new MapAssociationSnapshot( atomicMap ) );
	}

	@Override
	public void updateAssociation(Association association, AssociationKey key, AssociationContext associationContext) {
		MapHelpers.updateAssociation( association, key );
	}

	@Override
	public void removeAssociation(AssociationKey key, AssociationContext associationContext) {
		RemoteCache<AssociationKey, Map<RowKey, Map<String, Object>>> cache = provider.getCache( ASSOCIATION_STORE );
		HotRodAtomicMapLookup.removeAtomicMap( cache, key );
	}

	@Override
	public Tuple createTupleAssociation(AssociationKey associationKey, RowKey rowKey) {
		return new Tuple( EmptyTupleSnapshot.INSTANCE );
	}

	@Override
	public void nextValue(RowKey key, IntegralDataTypeHolder value, int increment, int initialValue) {
		final RemoteCache<RowKey, Object> identifierCache = provider.getCache( IDENTIFIER_STORE );
		boolean done = false;
		do {
			// read value
			VersionedValue<Object> versionedFromDb = identifierCache.getVersioned( key );
			if ( versionedFromDb == null ) {
				// if not there, insert initial value
				value.initialize( initialValue );
				// TODO should we use GridTypes here?
				Object valueFromDB = new Long( value.makeValue().longValue() );
				// update value
				final IntegralDataTypeHolder updateValue = value.copy();
				// increment value
				updateValue.add( increment );
				final Object newValueFromDb = updateValue.makeValue();
				final Object oldValue = identifierCache.putIfAbsent( key, newValueFromDb );
				done = oldValue == null;
			}
			else {
				// read the value from the table
				value.initialize( ( (Number) versionedFromDb.getValue() ).longValue() );
				final IntegralDataTypeHolder updateValue = value.copy();
				// increment value
				updateValue.add( increment );
				done = identifierCache.replaceWithVersion( key, updateValue.makeValue(), versionedFromDb.getVersion() );
			}
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
