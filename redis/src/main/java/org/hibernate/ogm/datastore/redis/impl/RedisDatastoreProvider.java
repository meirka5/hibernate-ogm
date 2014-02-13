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

package org.hibernate.ogm.datastore.redis.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.spi.Association;
import org.hibernate.ogm.datastore.spi.AssociationContext;
import org.hibernate.ogm.datastore.spi.AssociationOperation;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.datastore.spi.TupleContext;
import org.hibernate.ogm.datastore.spi.TupleOperation;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.dialect.redis.RedisDialect;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.logging.redis.impl.Log;
import org.hibernate.ogm.logging.redis.impl.LoggerFactory;
import org.hibernate.ogm.service.impl.LuceneBasedQueryParserService;
import org.hibernate.ogm.service.impl.QueryParserService;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

/**
 * @author Seiya Kawashima <skawashima@uchicago.edu>
 */
public class RedisDatastoreProvider implements DatastoreProvider, Startable, Stoppable, Configurable {

	private static final Log log = LoggerFactory.getLogger();

	private JedisPool pool;

	private RedisConfiguration configuration;

	@Override
	public void configure(Map configurationValues) {
		configuration = new RedisConfiguration( configurationValues );
	}

	@Override
	public void start() {
		try {
			log.redisStarting();
			pool = new JedisPool(
					configuration.buildJedisPoolConfig(),
					configuration.getHost(),
					configuration.getPort(),
					Protocol.DEFAULT_TIMEOUT,
					configuration.getPassword(),
					configuration.getDatabaseIndex()
					);
		}
		catch (Exception ex) {
			throw log.cannotStartRedis( ex );
		}
	}

	@Override
	public void stop() {
		log.redisStopping();
		if ( pool != null ) {
			pool.destroy();
		}
	}

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return RedisDialect.class;
	}

	public JedisPool getPool() {
		return pool;
	}

	public Map<String, Object> getEntityTuple(EntityKey entityKey, TupleContext context) {
		Jedis jedis = pool.getResource();
		try {
			Transaction tx = jedis.multi();
			String[] columns = null;
			if ( context == null ) {
				columns = entityKey.getColumnNames();
			}
			else {
				List<String> selectableColumns = context.getSelectableColumns();
				columns = new String[entityKey.getColumnNames().length + selectableColumns.size()];
				int keySize = entityKey.getColumnNames().length;
				System.arraycopy( entityKey.getColumnNames(), 0, columns, 0, keySize );
				System.arraycopy( selectableColumns.toArray( new String[selectableColumns.size()] ), 0, columns, keySize, selectableColumns.size() );
			}
			String key = entityKey.toString();
			Response<List<String>> list = tx.hmget( key, columns );
			tx.exec();
			List<String> results = list.get();
			return convert( columns, results );
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	private Map<String, Object> convert(String[] keys, List<String> values) {
		Map<String, Object> resultsString = new HashMap<String, Object>();
		for ( int i = 0; i < keys.length; i++ ) {
			String value = values.get( i );
			if ( value != null ) {
				resultsString.put( keys[i], value );
			}
		}
		if ( resultsString.isEmpty() ) {
			return null;
		}
		return resultsString;
	}

	private Map<String, Object> convertForAssociation(String[] keys, List<String> values) {
		Map<String, Object> resultsString = new HashMap<String, Object>();
		for ( int i = 0; i < keys.length; i++ ) {
			String value = values.get( i );
			if ( value != null ) {
				resultsString.put( keys[i], value );
			}
		}
		if ( resultsString.isEmpty() ) {
			return null;
		}
		return resultsString;
	}

	public void putEntity(EntityKey key, Tuple tuple) {
		Jedis jedis = pool.getResource();
		try {
			Transaction tx = jedis.multi();
			for ( TupleOperation action : tuple.getOperations() ) {
				switch ( action.getType() ) {
					case PUT:
						tx.hset( key.toString(), action.getColumn(), (String) action.getValue() );
						break;
					case PUT_NULL:
					case REMOVE:
						tx.hdel( key.toString(), action.getColumn() );
						break;
				}
			}
			tx.exec();
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	public void removeEntity(EntityKey key) {
		Jedis jedis = pool.getResource();
		try {
			Transaction tx = jedis.multi();
			Response<Map<String, String>> hgetAll = tx.hgetAll( key.toString() );
			tx.exec();
			Map<String, String> map = hgetAll.get();
			tx = jedis.multi();
			String[] keySet = map.keySet().toArray( new String[map.keySet().size()] );
			tx.hdel( key.toString(), keySet );
			tx.exec();
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	public Map<RowKey, Map<String, Object>> getAssociation(AssociationKey key, AssociationContext context) {
		Jedis jedis = pool.getResource();
		try {
			Transaction tx = jedis.multi();
			Response<Set<String>> smembers = tx.smembers( key.toString() );
			tx.exec();
			Set<String> rowKeys = smembers.get();
			String[] rowKeyColumnNames = key.getRowKeyColumnNames();
			Map<RowKey, Map<String, Object>> result = new HashMap<RowKey, Map<String, Object>>();
			for ( String rowKey : rowKeys ) {
				tx = jedis.multi();
				Response<List<String>> list = tx.hmget( rowKey, rowKeyColumnNames );
				tx.exec();
				List<String> rowKeyValues = list.get();
				RowKey rk = new RowKey( key.getTable(), rowKeyColumnNames, rowKeyValues.toArray() );
				Map<String, Object> convert = convertForAssociation( rowKeyColumnNames, rowKeyValues );
				result.put( rk, convert );
			}
			if ( result.isEmpty() ) {
				return null;
			}
			return result;
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	public void putAssociation(Association association, AssociationKey key) {
		Jedis jedis = pool.getResource();
		try {
			Transaction tx = jedis.multi();
			for ( AssociationOperation action : association.getOperations() ) {
				switch ( action.getType() ) {
					case PUT:
						tx.sadd( key.toString(), action.getKey().toString() );
						putAssociation( tx,key, action );
						break;
					case PUT_NULL:
					case REMOVE:
						tx.hdel( action.getKey().toString(), action.getKey().getColumnNames() );
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
		Tuple tuple = action.getValue();
		if ( tuple != null ) {
			for ( TupleOperation tupleOperation : tuple.getOperations() ) {
				RowKey rowKey = action.getKey();
				switch ( tupleOperation.getType() ) {
					case PUT:
						tx.hset( rowKey.toString(), tupleOperation.getColumn(), (String) tupleOperation.getValue() );
						break;
					case PUT_NULL:
					case REMOVE:
						tx.hdel( rowKey.toString(), tupleOperation.getColumn() );
						break;
				}
			}
		}
	}

	public void removeAssociation(AssociationKey key) {
		Jedis jedis = pool.getResource();
		try {
			Transaction tx = jedis.multi();
			Response<Set<String>> smembers = tx.smembers( key.toString() );
			tx.exec();
			Set<String> set = smembers.get();
			String[] array = set.toArray( new String[set.size()] );
			tx = jedis.multi();
			tx.srem( key.toString(), array );
			tx.exec();
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	public void setNextValue(RowKey rowKey, IntegralDataTypeHolder value, int increment, int initialValue) {
		String sequenceId = rowKey.toString();
		Jedis jedis = pool.getResource();
		jedis = pool.getResource();
		try {
			Transaction tx = jedis.multi();
			Response<String> sequenceValue = tx.get( sequenceId );
			tx.exec();
			tx.watch( sequenceId );
			tx = jedis.multi();
			if ( sequenceValue.get() == null ) {
				tx.set( sequenceId, String.valueOf( initialValue ) );
				tx.exec();
				value.initialize( initialValue );
			}
			else {
				Response<Long> incrBy = tx.incrBy( sequenceId, increment );
				tx.exec();
				value.initialize( incrBy.get() );
			}
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	@Override
	public Class<? extends QueryParserService> getDefaultQueryParserServiceType() {
		return LuceneBasedQueryParserService.class;
	}

}
