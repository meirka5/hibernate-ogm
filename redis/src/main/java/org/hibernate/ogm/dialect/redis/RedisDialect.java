/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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

package org.hibernate.ogm.dialect.redis;

import static org.hibernate.ogm.dialect.redis.DomainSpace.ENTITY;
import static org.hibernate.ogm.dialect.redis.DomainSpace.ASSOCIATION;
import static org.hibernate.ogm.dialect.redis.DomainSpace.ASSOCIATION_ROW;
import static org.hibernate.ogm.dialect.redis.DomainSpace.SEQUENCE;
import static org.hibernate.ogm.dialect.redis.RedisIdentifier.createId;
import static org.hibernate.ogm.dialect.redis.RedisIdentifier.createPrefix;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.hibernate.LockMode;
import org.hibernate.dialect.lock.LockingStrategy;
import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.loader.custom.CustomQuery;
import org.hibernate.ogm.datastore.redis.impl.RedisDatastoreProvider;
import org.hibernate.ogm.datastore.spi.Association;
import org.hibernate.ogm.datastore.spi.AssociationContext;
import org.hibernate.ogm.datastore.spi.AssociationOperation;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.datastore.spi.TupleContext;
import org.hibernate.ogm.datastore.spi.TupleOperation;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.exception.NotSupportedException;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.EntityKeyMetadata;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.massindex.batchindexing.Consumer;
import org.hibernate.ogm.type.GridType;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.type.Type;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

/**
 * @author Seiya Kawashima <skawashima@uchicago.edu>
 */
public class RedisDialect implements GridDialect {

	private final RedisDatastoreProvider provider;

	public RedisDialect(RedisDatastoreProvider provider) {
		this.provider = provider;
	}

	@Override
	public LockingStrategy getLockingStrategy(Lockable lockable, LockMode lockMode) {
		// TODO Implementing this method needs help from Redis community. Once figuring out how to map lock strategies
		// with Redis, this method will be implemented. Until that time, this method simply throws an exception.
		throw new RuntimeException( "the lock is not supported yet." );
	}

	@Override
	public Tuple getTuple(EntityKey entityKey, TupleContext context) {
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		try {
			Map<String, String> entityMap = jedis.hgetAll( createId( ENTITY, entityKey ) );
			if ( entityMap.isEmpty() ) {
				return null;
			}
			return new Tuple( new RedisTupleSnapshot( entityMap ) );
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	@Override
	public Tuple createTuple(EntityKey key) {
		return new Tuple();
	}

	@Override
	public void updateTuple(Tuple tuple, EntityKey entityKey) {
		String key = createId( ENTITY, entityKey );
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		try {
			jedis.watch( key );
			Transaction tx = jedis.multi();
			for ( TupleOperation action : tuple.getOperations() ) {
				switch ( action.getType() ) {
					case PUT:
						tx.hset( key, action.getColumn(), (String) action.getValue() );
						break;
					case PUT_NULL:
					case REMOVE:
						tx.hdel( key, action.getColumn() );
						break;
				}
			}
			tx.exec();
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	@Override
	public void removeTuple(EntityKey entityKey) {
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		try {
			jedis.del( createId( ENTITY, entityKey ) );
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	@Override
	public Association getAssociation(AssociationKey associationKey, AssociationContext context) {
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		try {
			Set<String> rowKeys = jedis.smembers( createId( ASSOCIATION, associationKey ) );
			String[] rowKeyColumnNames = associationKey.getRowKeyColumnNames();
			Map<RowKey, Map<String, String>> result = new HashMap<RowKey, Map<String, String>>();
			for ( String rowKey : rowKeys ) {
				Map<String, String> associationValues = jedis.hgetAll( rowKey );
				RowKey rk = createRowKey( associationKey, rowKeyColumnNames, associationValues );
				result.put( rk, associationValues );
			}
			if ( result.isEmpty() ) {
				return null;
			}
			return new Association( new RedisAssociationSnapshot( result ) );
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	private RowKey createRowKey(AssociationKey key, String[] rowKeyColumnNames, Map<String, String> associationValues) {
		Object[] rowKeyValues = new Object[rowKeyColumnNames.length];
		for ( int i = 0; i < rowKeyColumnNames.length; i++ ) {
			rowKeyValues[i] = associationValues.get( rowKeyColumnNames[i] );
		}
		return new RowKey( key.getTable(), rowKeyColumnNames, rowKeyValues );
	}

	@Override
	public Association createAssociation(AssociationKey key, AssociationContext context) {
		return new Association( new RedisAssociationSnapshot() );
	}

	@Override
	public void updateAssociation(Association association, AssociationKey associationKey, AssociationContext context) {
		String key = createId( ASSOCIATION, associationKey );
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		try {
			jedis.watch( key );
			Transaction tx = jedis.multi();
			for ( AssociationOperation action : association.getOperations() ) {
				switch ( action.getType() ) {
					case PUT:
						tx.sadd( key, createId( ASSOCIATION_ROW, action.getKey() ) );
						putAssociation( tx, associationKey, action );
						break;
					case PUT_NULL:
					case REMOVE:
						String rowKey = createId( ASSOCIATION_ROW, action.getKey() );
						tx.srem( key, rowKey );
						tx.del( rowKey );
						break;
				}
			}
			tx.exec();
 		}
		finally {
			pool.returnResource( jedis );
		}
	}

	private void putAssociation(Transaction tx, AssociationKey associationKey, AssociationOperation action) {
		String rowKey = createId(ASSOCIATION_ROW, action.getKey());
		Tuple tuple = action.getValue();
		if ( tuple != null ) {
			for ( TupleOperation tupleOperation : tuple.getOperations() ) {
				switch ( tupleOperation.getType() ) {
					case PUT:
						tx.hset( rowKey, tupleOperation.getColumn(), (String) tupleOperation.getValue() );
						break;
					case PUT_NULL:
					case REMOVE:
						tx.hdel( rowKey, tupleOperation.getColumn() );
						break;
				}
			}
		}
	}

	@Override
	public void removeAssociation(AssociationKey key, AssociationContext context) {
		String id = createId( ASSOCIATION, key );
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		try {
			jedis.watch( id );
			Set<String> rowKeys = jedis.smembers( id );
			Transaction tx = jedis.multi();
			String[] members = rowKeys.toArray( new String[rowKeys.size()] );
			if (members.length > 0) {
				tx.srem( id, members );
				tx.del( members );
			}
			tx.del( id );
			tx.exec();
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	@Override
	public Tuple createTupleAssociation(AssociationKey associationKey, RowKey rowKey) {
		return new Tuple();
	}

	@Override
	public void nextValue(RowKey key, IntegralDataTypeHolder value, int increment, int initialValue) {
		String sequenceId = createId( SEQUENCE, key );
		int nextValue = nextValue( increment, initialValue, createId( SEQUENCE, key ) );
		value.initialize( nextValue );
	}

	private int nextValue(int increment, int initialValue, String sequenceId) {
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		try {
			Long response = jedis.setnx( sequenceId, String.valueOf( initialValue ) );
			boolean created = response.equals( 1L );
			if ( created ) {
				return initialValue;
			}
			else {
				Long newValue = jedis.incrBy( sequenceId, increment );
				return newValue.intValue();
			}
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	@Override
	public GridType overrideType(Type type) {
		return TypeConverter.INSTANCE.convert( type );
	}

	@Override
	public void forEachTuple(Consumer consumer, EntityKeyMetadata... entityKeyMetadatas) {
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		for ( EntityKeyMetadata entityKeyMetadata : entityKeyMetadatas ) {
			try {
				String generatePrefix = createPrefix( ENTITY, entityKeyMetadata.getTable() );
				Set<String> keys = jedis.keys( generatePrefix + "*" );
				for ( String entityKey : keys ) {
					Map<String, String> tupleMap = jedis.hgetAll( entityKey );
					Tuple tuple = new Tuple( new RedisTupleSnapshot( tupleMap ) );
					consumer.consume( tuple );
				}
			}
			finally {
				pool.returnResource( jedis );
			}
		}
	}

	@Override
	public Iterator<Tuple> executeBackendQuery(CustomQuery customQuery, EntityKeyMetadata[] metadatas) {
		throw new NotSupportedException( "TBD", "Native queries not supported for Redis" );
	}

}
