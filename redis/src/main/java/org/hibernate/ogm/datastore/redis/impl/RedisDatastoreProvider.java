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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.ClassUtils;
import org.hibernate.HibernateException;
import org.hibernate.cfg.Environment;
import org.hibernate.id.IntegralDataTypeHolder;
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
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

/**
 * @author Seiya Kawashima <skawashima@uchicago.edu>
 */
public class RedisDatastoreProvider implements DatastoreProvider, Startable, Stoppable {

	private static final Log log = LoggerFactory.getLogger();
	private static final String PROPERTY_PREFIX = "hibernate.datastore.provider.redis_config.";
	private static final String ASSOCIATION_HSET = "OGM-Association";
	private static final String SEQUENCE_HSET = "OGM-Sequence";

	private Map<String, String> requiredProperties;
	private Pattern setterPattern;
	private JedisPool pool;

	private static final String SEQUENCE_LABEL = "nextSequence";

	private static enum RequiredProp {
		PROVIDER_URL("provider_url", "hibernate.ogm.datastore.provider_url");

		private String name;
		private String propPath;

		RequiredProp(String name, String propPath) {
			this.name = name;
			this.propPath = propPath;
		}

		public String getName() {
			return name;
		}

		public String getPropPath() {
			return propPath;
		}
	}

	@Override
	public void start() {
		try {
			Map<String, String> unSetRequiredProperties = checkRequiredSettings();
			if ( !unSetRequiredProperties.isEmpty() ) {
				throw log.missingPropertiesAtStartup( unSetRequiredProperties.toString() );
			}
			log.redisStarting();
			setUpRedis();
		}
		catch (Exception ex) {
			stop();
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

	/**
	 * Reused from VoldemortDatastoreProvider. Checks required property settings for Redis on hibernate.properties.
	 *
	 * @return Empty map if the required properties set correctly. Otherwise return a map containing unset required
	 * properties whose keys are the required properties' names and values are the corresponding values.
	 */
	private synchronized Map<String, String> checkRequiredSettings() {
		requiredProperties = getRequiredPropertyValues();

		Map<String, String> unSetRequiredProperties = new HashMap<String, String>();

		if ( requiredProperties.get( RequiredProp.PROVIDER_URL.getName() ) == null || requiredProperties.get( RequiredProp.PROVIDER_URL.getName() ).equals( "" ) ) {
			unSetRequiredProperties.put( RequiredProp.PROVIDER_URL.getName(), null );
		}

		return unSetRequiredProperties;
	}

	/**
	 * Reused from VoldemortDatastoreProvider. Gets the common required property values among other datastore provider
	 * and the datastore specific property values.
	 *
	 * @return Key value pairs storing the properties.
	 */
	private Map<String, String> getRequiredPropertyValues() {
		Map<String, String> map = new HashMap<String, String>();
		map.put( RequiredProp.PROVIDER_URL.getName(), Environment.getProperties().getProperty( RequiredProp.PROVIDER_URL.getPropPath() ) );

		return Collections.unmodifiableMap( map );
	}

	private synchronized void setUpRedis() {
		pool = new JedisPool( createJedisConfig(), requiredProperties.get( RequiredProp.PROVIDER_URL.getName() ) );
	}

	/**
	 * Reads Redis configuration properties from hibernate.properties and creates JedisPoolConfig object.
	 *
	 * @return Newly created JedisPoolConfig object.
	 */
	private final JedisPoolConfig createJedisConfig() {
		final JedisPoolConfig jedisConfig = new JedisPoolConfig();
		Method[] methods = jedisConfig.getClass().getDeclaredMethods();
		Entry<Object, Object> entry = null;
		String key = "";
		for ( Iterator<Entry<Object, Object>> itr = Environment.getProperties().entrySet().iterator(); itr.hasNext(); ) {
			entry = itr.next();
			key = (String) entry.getKey();
			if ( key.startsWith( PROPERTY_PREFIX ) ) {
				setConfigProperty( key, (String) entry.getValue(), methods, jedisConfig );
			}
		}
		return jedisConfig;
	}

	/**
	 * Sets Redis properties on hibernate.properties started with "hibernate.datastore.provider.redis_config." using the
	 * corresponding setter methods on the parameter, jedisConfig. Please check the javadoc at
	 * http://www.jarvana.com/jarvana/view/redis/clients/jedis/2.0.0/jedis-2.0.0-javadoc.jar!/index.html
	 *
	 * @param key Name of the property.
	 * @param value Value of the property.
	 * @param methods Methods declared on JedisPoolConfig.
	 * @param jedisConfig Where the value is set calling the corresponding setter.
	 */
	private void setConfigProperty(String key, String value, Method[] methods, JedisPoolConfig jedisConfig) {

		String propertyName = key.substring( key.lastIndexOf( "." ) + 1 );
		for ( Method method : methods ) {
			setterPattern = Pattern.compile( "^set" + propertyName + "$", Pattern.CASE_INSENSITIVE );
			if ( hasPattern( method.getName(), setterPattern ) ) {
				callSetter( value, method, jedisConfig );
			}
		}
	}

	/**
	 * Calls setter method.
	 *
	 * @param value Value to be set through the setter.
	 * @param method Setter.
	 * @param jedisConfig Where the setter with the value is called.
	 */
	private void callSetter(String value, Method method, JedisPoolConfig jedisConfig) {
		Class[] parameters = method.getParameterTypes();
		if ( parameters.length != 1 ) {
			throw new RuntimeException( "the parameter for setter should be length == 1, but found " + parameters.length + " on " + method.getName() );
		}

		try {
			Object targetObj = createWrapperObject( value, parameters[0] );
			method.invoke( jedisConfig, targetObj.getClass().getDeclaredMethod( parameters[0].getCanonicalName() + "Value" ).invoke( targetObj ) );
		}
		catch (Exception ex) {
			throwHibernateExceptionFrom( ex );
		}
	}

	/**
	 * Creates wrapper object based on the parameters.
	 *
	 * @param initValue Initial value to be set when the wrapper object is constructed.
	 * @param primitiveClass Class representing primitive type.
	 * @return Newly created wrapper object.
	 */
	private Object createWrapperObject(String initValue, Class primitiveClass) {
		Class wrapperClass = ClassUtils.primitiveToWrapper( primitiveClass );
		try {
			Constructor ctor = wrapperClass.getDeclaredConstructor( String.class );
			return ctor.newInstance( initValue );
		}
		catch (Exception ex) {
			throwHibernateExceptionFrom( ex );
			return null;
		}
	}

	/**
	 * Copied from AnnotationFinder. Checks if the parameter, str has the parameter, pattern.
	 * 
	 * @param str String to be examined.
	 * @param pattern Used to check the parameter, str.
	 * @return True if patter is included.
	 */
	private boolean hasPattern(String str, Pattern pattern) {

		Matcher matcher = pattern.matcher( str );
		while ( matcher.find() ) {
			return true;
		}

		return false;
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
			} else {
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

	/**
	 * Reused from VoldemortDatastoreProvider. Gets the declared fields from the specified class.
	 *
	 * @param className Class name used to get the declared fields.
	 * @return Field array storing the declared fields.
	 */
	private Field[] getDeclaredFieldsFrom(String className) {
		Field[] fields = null;
		try {
			fields = Class.forName( className ).getDeclaredFields();
		}
		catch (SecurityException e) {
			throwHibernateExceptionFrom( e );
		}
		catch (ClassNotFoundException e) {
			throwHibernateExceptionFrom( e );
		}
		return fields;
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
			Map<RowKey, Map<String, Object>> result =  new HashMap<RowKey, Map<String,Object>>();
			for ( String rowKey : rowKeys ) {
				tx = jedis.multi();
				Response<List<String>> list = tx.hmget( rowKey, rowKeyColumnNames );
				tx.exec();
				List<String> rowKeyValues = list.get();
				RowKey rk = new RowKey( key.getTable(), rowKeyColumnNames, rowKeyValues.toArray() );
				Map<String, Object> convert = convertForAssociation( rowKeyColumnNames, rowKeyValues );
				result.put( rk, convert );
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
						putAssociation( tx, action );
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

	private void putAssociation(Transaction tx, AssociationOperation action) {
		Tuple tuple = action.getValue();
		if (tuple != null) {
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

	/**
	 * Gets association key as Map object containing owning columns.
	 *
	 * @return Association key as Map representation.
	 */
	public Map<String, Object> getAssociationKeyAsMap(AssociationKey associationKey) {

		Map<String, Object> map = new HashMap<String, Object>();
		for ( int i = 0; i < associationKey.getColumnNames().length; i++ ) {
			map.put( associationKey.getColumnNames()[i], associationKey.getColumnValues()[i] );
		}
		map.put( "table", associationKey.getTable() );

		return Collections.unmodifiableMap( map );
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
			}  else {
				Response<Long> incrBy = tx.incrBy( sequenceId, increment );
				tx.exec();
				value.initialize( incrBy.get() );
			}
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	public Map<AssociationKey, Map<RowKey, Map<String, Object>>> getAssociationsMap() {
		Map<AssociationKey, Map<RowKey, Map<String, Object>>> associations = new HashMap<AssociationKey, Map<RowKey, Map<String, Object>>>();
		Response<Map<String, String>> res = null;
		Jedis jedis = pool.getResource();
		try {
			Transaction tx = jedis.multi();
			res = tx.hgetAll( ASSOCIATION_HSET );
			tx.exec();
		}
		finally {
			pool.returnResource( jedis );
		}

		Entry<String, String> entry = null;
		for ( Iterator<Entry<String, String>> itr = res.get().entrySet().iterator(); itr.hasNext(); ) {
			entry = itr.next();
		}
		return associations;
	}

	/**
	 * Converts the specified exception to HibernateException and rethrows it.
	 *
	 * @param <T>
	 * @param exception Exception to be rethrown as HibernateException.
	 */
	protected <T extends Throwable> void throwHibernateExceptionFrom(T exception) {
		throw new HibernateException( exception );
	}

	@Override
	public Class<? extends QueryParserService> getDefaultQueryParserServiceType() {
		return LuceneBasedQueryParserService.class;
	}
}
