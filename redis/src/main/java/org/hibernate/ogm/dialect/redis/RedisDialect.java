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
import org.hibernate.ogm.dialect.redis.type.RedisBooleanType;
import org.hibernate.ogm.dialect.redis.type.RedisByteType;
import org.hibernate.ogm.dialect.redis.type.RedisDoubleType;
import org.hibernate.ogm.dialect.redis.type.RedisIntegerType;
import org.hibernate.ogm.dialect.redis.type.RedisLongType;
import org.hibernate.ogm.dialect.redis.type.RedisPrimitiveByteType;
import org.hibernate.ogm.exception.NotSupportedException;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.EntityKeyMetadata;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.massindex.batchindexing.Consumer;
import org.hibernate.ogm.type.BigDecimalType;
import org.hibernate.ogm.type.BigIntegerType;
import org.hibernate.ogm.type.GridType;
import org.hibernate.ogm.type.Iso8601StringCalendarType;
import org.hibernate.ogm.type.Iso8601StringDateType;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.Type;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
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
	public Tuple getTuple(EntityKey key, TupleContext context) {
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		try {
			Transaction tx = jedis.multi();
			Response<Map<String, String>> response = tx.hgetAll( key.toString() );
			tx.exec();
			Map<String, String> entityMap = response.get();
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
	public void updateTuple(Tuple tuple, EntityKey key) {
		String id = key.toString();
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		try {
			jedis.watch( id );
			Transaction tx = jedis.multi();
			for ( TupleOperation action : tuple.getOperations() ) {
				switch ( action.getType() ) {
					case PUT:
						tx.hset( id, action.getColumn(), (String) action.getValue() );
						break;
					case PUT_NULL:
					case REMOVE:
						tx.hdel( id, action.getColumn() );
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
	public void removeTuple(EntityKey key) {
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		try {
			jedis.del( key.toString() );
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	@Override
	public Association getAssociation(AssociationKey key, AssociationContext context) {
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		try {
			Set<String> rowKeys = jedis.smembers( key.toString() );
			String[] rowKeyColumnNames = key.getRowKeyColumnNames();
			Map<RowKey, Map<String, String>> result = new HashMap<RowKey, Map<String, String>>();
			for ( String rowKey : rowKeys ) {
				Map<String, String> associationValues = jedis.hgetAll( rowKey );
				RowKey rk = createRowKey( key, rowKeyColumnNames, associationValues );
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
	public void updateAssociation(Association association, AssociationKey key, AssociationContext context) {
		String associationId = key.toString();
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		try {
			jedis.watch( associationId );
			Transaction tx = jedis.multi();
			for ( AssociationOperation action : association.getOperations() ) {
				switch ( action.getType() ) {
					case PUT:
						tx.sadd( associationId, action.getKey().toString() );
						putAssociation( tx, key, action );
						break;
					case PUT_NULL:
					case REMOVE:
						tx.srem( associationId, action.getKey().toString() );
						tx.del( action.getKey().toString() );
						break;
				}
			}
			tx.exec();
 		}
		finally {
			pool.returnResource( jedis );
		}
	}

	private void putAssociation(Transaction tx, AssociationKey key, AssociationOperation action) {
		String rowKey = action.getKey().toString();
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
		String id = key.toString();
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
		String sequenceId = key.toString();
		int nextValue = nextValue( increment, initialValue, sequenceId );
		value.initialize( nextValue );
	}

	private int nextValue(int increment, int initialValue, String sequenceId) {
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		try {
			Long response = jedis.setnx( sequenceId, String.valueOf( initialValue ) );
			boolean created = response.equals( 1L );
			if ( created ) {
				System.out.println( Thread.currentThread().getId() + " initial: " + initialValue );
				return initialValue;
			}
			else {
				Long newValue = jedis.incrBy( sequenceId, increment );
				System.out.println( Thread.currentThread().getId() + " newvalue: " + newValue );
				return newValue.intValue();
			}
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	@Override
	public GridType overrideType(Type type) {
		if ( type == StandardBasicTypes.BIG_DECIMAL ) {
			return BigDecimalType.INSTANCE;
		}
		if ( type == StandardBasicTypes.BIG_INTEGER ) {
			return BigIntegerType.INSTANCE;
		}
		// persist calendars as ISO8601 strings, including TZ info
		else if ( type == StandardBasicTypes.CALENDAR ) {
			return Iso8601StringCalendarType.DATE_TIME;
		}
		else if ( type == StandardBasicTypes.CALENDAR_DATE ) {
			return Iso8601StringCalendarType.DATE;
		}
		// persist date as ISO8601 strings, in UTC, without TZ info
		else if ( type == StandardBasicTypes.DATE ) {
			return Iso8601StringDateType.DATE;
		}
		else if ( type == StandardBasicTypes.TIME ) {
			return Iso8601StringDateType.TIME;
		}
		else if ( type == StandardBasicTypes.TIMESTAMP ) {
			return Iso8601StringDateType.DATE_TIME;
		}
		else if ( type == StandardBasicTypes.BYTE ) {
			return RedisByteType.INSTANCE;
		}
		else if ( type == StandardBasicTypes.LONG ) {
			return RedisLongType.INSTANCE;
		}
		else if ( type == StandardBasicTypes.INTEGER ) {
			return RedisIntegerType.INSTANCE;
		}
		else if ( type == StandardBasicTypes.DOUBLE ) {
			return RedisDoubleType.INSTANCE;
		}
		else if ( type == StandardBasicTypes.BOOLEAN ) {
			return RedisBooleanType.INSTANCE;
		}
		else if ( type == StandardBasicTypes.MATERIALIZED_BLOB ) {
			return RedisPrimitiveByteType.INSTANCE;
		}
		return null;
	}

	@Override
	public void forEachTuple(Consumer consumer, EntityKeyMetadata... entityKeyMetadatas) {
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		for ( EntityKeyMetadata entityKeyMetadata : entityKeyMetadatas ) {
			try {
				Transaction tx = jedis.multi();
				Response<Set<String>> keys = tx.keys( "EntityKey{table='" + entityKeyMetadata.getTable() + "'*" );
				tx.exec();
				for ( String entityKey : keys.get() ) {
					tx = jedis.multi();
					Response<Map<String, String>> tupleMap = tx.hgetAll( entityKey );
					tx.exec();
					Tuple tuple = new Tuple( new RedisTupleSnapshot( tupleMap.get() ) );
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
