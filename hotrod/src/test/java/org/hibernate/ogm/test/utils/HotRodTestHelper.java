/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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
package org.hibernate.ogm.test.utils;

import static org.hibernate.ogm.datastore.spi.DefaultDatastoreNames.ASSOCIATION_STORE;
import static org.hibernate.ogm.datastore.spi.DefaultDatastoreNames.ENTITY_STORE;
import static org.hibernate.ogm.datastore.spi.DefaultDatastoreNames.IDENTIFIER_STORE;

import java.util.Map;
import java.util.Set;

import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.datastore.hotrod.impl.HotRodDatastoreProvider;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.grid.EntityKey;
import org.infinispan.client.hotrod.RemoteCache;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
public class HotRodTestHelper implements TestableGridDialect {

	@Override
	public boolean assertNumberOfEntities(int numberOfEntities, SessionFactory sessionFactory) {
		RemoteCache entityCache = getEntityCache( sessionFactory );
		Set entrySet = entityCache.keySet();
		int size = entityCache.size();
		return size == numberOfEntities;
	}

	@Override
	public boolean assertNumberOfAssociations(int numberOfAssociations, SessionFactory sessionFactory) {
		return getAssociationCache( sessionFactory ).size() == numberOfAssociations;
	}

	@Override
	public Map<String, Object> extractEntityTuple(SessionFactory sessionFactory, EntityKey key) {
		return (Map) getEntityCache( sessionFactory ).get( key );
	}

	private static RemoteCache getEntityCache(SessionFactory sessionFactory) {
		HotRodDatastoreProvider castProvider = getProvider( sessionFactory );
		return castProvider.getCache( ENTITY_STORE );
	}

	public static HotRodDatastoreProvider getProvider(SessionFactory sessionFactory) {
		DatastoreProvider provider = ( (SessionFactoryImplementor) sessionFactory ).getServiceRegistry().getService( DatastoreProvider.class );
		if ( !( HotRodDatastoreProvider.class.isInstance( provider ) ) ) {
			throw new RuntimeException( "Not testing with Infinispan, cannot extract underlying cache" );
		}
		return HotRodDatastoreProvider.class.cast( provider );
	}

	private static RemoteCache getAssociationCache(SessionFactory sessionFactory) {
		HotRodDatastoreProvider castProvider = getProvider( sessionFactory );
		return castProvider.getCache( ASSOCIATION_STORE );
	}

	@Override
	public boolean backendSupportsTransactions() {
		return false;
	}

	@Override
	public void dropSchemaAndDatabase(SessionFactory sessionFactory) {
		HotRodDatastoreProvider castProvider = getProvider( sessionFactory );
		clear( castProvider.getCache( ENTITY_STORE ) );
		clear( castProvider.getCache( ASSOCIATION_STORE ) );
		clear( castProvider.getCache( IDENTIFIER_STORE ) );
	}

	private void clear(RemoteCache cache) {
		Set keySet = cache.keySet();
		for ( Object key : keySet ) {
			cache.remove( key );
		}
	}

	@Override
	public Map<String, String> getEnvironmentProperties() {
		return null;
	}

}