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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.ClassUtils;
import org.hibernate.HibernateException;
import org.hibernate.cfg.Environment;
import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.datastore.spi.TupleContext;
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
	private Map<String, String> requiredProperties;
	private Pattern setterPattern;
	private JedisPool pool;
	private Jedis jedis;
	private static final String PROPERTY_PREFIX = "hibernate.datastore.provider.redis_config.";
	private static final String ASSOCIATION_HSET = "OGM-Association";
	private static final String SEQUENCE_HSET = "OGM-Sequence";

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

	public JSONHelper getJsonHelper() {
		return jsonHelper;
	}

	public Map<String, String> getEntityTuple(EntityKey entityKey, TupleContext context) {
		jedis = pool.getResource();
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

	private Map<String, String> convert(String[] keys, List<String> values) {
		Map<String, String> resultsString = new HashMap<String, String>();
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

	public void putEntity(EntityKey key, Map<String, String> tuple) {
		jedis = pool.getResource();
		try {
			Transaction tx = jedis.multi();
			tx.hmset( key.toString(), tuple );
			tx.exec();
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	/**
	 * Gets entity key as Map containing id and table name.
	 *
	 * @return Map containing id and table name.
	 */
	public Map<String, String> getEntityKeyAsMap(EntityKey entityKey) {
		Map<String, String> map = new HashMap<String, String>();
		map.put( "id", entityKey.toString() );
		map.put( "table", entityKey.getTable() );
		return Collections.unmodifiableMap( map );
	}

	public void removeEntity(EntityKey key) {
		jedis = pool.getResource();
		try {
			Transaction tx = jedis.multi();
			Response<Map<String, String>> hgetAll = tx.hgetAll( key.toString() );
			tx.exec();			
			Map<String, String> map = hgetAll.get();
			tx = jedis.multi();
			String[] keySet = map.keySet().toArray( new String[map.keySet().size()] );
			Response<Long> hdel = tx.hdel( key.toString(), keySet );
			tx.exec();
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	public Map<RowKey, Map<String, Object>> getAssociation(AssociationKey key) {

		Response<String> res = null;
		boolean isNull = false;
		try {
			Transaction tx = jedis.multi();
			res = tx.hget( ASSOCIATION_HSET, jsonHelper.toJSON( getAssociationKeyAsMap( key ) ) );
			tx.exec();
		}
		catch (NullPointerException ex) {
			isNull = true;
		}
		catch (Exception ex) {
			throwHibernateExceptionFrom( ex );
		}
		finally {
			pool.returnResource( jedis );
		}

		if ( isNull ) {
			return null;
		}

		return createAssociationFrom( res.get() );
	}

	/**
	 * Copied from VoldemortDatastoreProvider. Creates association from the specified Jsoned string.
	 *
	 * @param jsonedAssociation Representation of association as JSON.
	 * @return Association based on the specified string.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map<RowKey, Map<String, Object>> createAssociationFrom(String jsonedAssociation) {

		Map associationMap = (Map) jsonHelper.fromJSON( jsonedAssociation, Map.class );
		Map<RowKey, Map<String, Object>> association = new HashMap<RowKey, Map<String, Object>>();
		String key = "";
		RowKey rowKey = null;
		for ( Iterator itr = associationMap.keySet().iterator(); itr.hasNext(); ) {
			key = (String) itr.next();
			rowKey = (RowKey) jsonHelper.fromJSON( key, RowKey.class );
			association.put( rowKey, jsonHelper.createAssociation( (String) associationMap.get( key ) ) );
		}

		return association;
	}

	public void putAssociation(AssociationKey key, Map<RowKey, Map<String, Object>> associationMap) {
		RollbackAction rollbackAction = new RollbackAction( this, key );
		try {
			Transaction tx = jedis.multi();
			Response<Long> res = tx.hset( ASSOCIATION_HSET, jsonHelper.toJSON( getAssociationKeyAsMap( key ) ),
					jsonHelper.toJSON( jsonHelper.convertKeyAndValueToJsonOn( associationMap ) ) );
			tx.exec();
		}
		catch (Exception ex) {
			rollbackAction.rollback();
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	public void removeAssociation(AssociationKey key) {
		RollbackAction rollbackAction = new RollbackAction( this, key );
		try {
			Transaction tx = jedis.multi();
			tx.hdel( ASSOCIATION_HSET, jsonHelper.toJSON( getAssociationKeyAsMap( key ) ) );
			tx.exec();
		}
		catch (Exception ex) {
			rollbackAction.rollback();
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

	public void setNextValue(RowKey key, IntegralDataTypeHolder value, int increment, int initialValue) {

		Integer seq = getSequence( key );

		if ( seq == null ) {

			synchronized ( this ) {
				value.initialize( initialValue );
			}

			Map<String, Integer> nextSequence = new HashMap<String, Integer>();
			nextSequence.put( SEQUENCE_LABEL, initialValue );
			putSequence( key, nextSequence );
		}
		else {
			int sequence = 0;

			synchronized ( this ) {
				sequence = seq + increment;
				value.initialize( sequence );
			}

			Map<String, Integer> nextSequence = new HashMap<String, Integer>();
			nextSequence.put( SEQUENCE_LABEL, sequence );
			putSequence( key, nextSequence );
		}
	}

	/**
	 * Gets sequence.
	 * 
	 * @param rowKey Used to search for the corresponding sequence.
	 * @return Sequence.
	 */
	public synchronized Integer getSequence(RowKey rowKey) {

		if ( rowKey == null ) {
			return null;
		}

		Response<String> res = null;
		boolean isNull = false;
		try {
			Transaction tx = jedis.multi();
			res = tx.hget( SEQUENCE_HSET, jsonHelper.toJSON( getRowKeyAsMap( rowKey ) ) );
			tx.exec();
		}
		catch (NullPointerException ex) {
			isNull = true;
		}
		catch (Exception ex) {
			throwHibernateExceptionFrom( ex );
		}
		finally {
			pool.returnResource( jedis );
		}

		if ( isNull ) {
			return null;
		}

		return (Integer) jsonHelper.get( res.get(), SEQUENCE_LABEL );
	}

	/**
	 * Puts sequence.
	 * 
	 * @param key
	 * @param nextSequence
	 */
	public synchronized void putSequence(RowKey key, Map<String, Integer> nextSequence) {

		RollbackAction rollbackAction = new RollbackAction( this, key );

		try {
			Transaction tx = jedis.multi();
			tx.hset( SEQUENCE_HSET, jsonHelper.toJSON( getRowKeyAsMap( key ) ), jsonHelper.toJSON( nextSequence ) );
			tx.exec();
		}
		catch (Exception ex) {
			rollbackAction.rollback();
			throwHibernateExceptionFrom( ex );
		}
		finally {
			pool.returnResource( jedis );
		}
	}

	/**
	 * Reused from VoldemortDialect. Gets row key as Map object containing owning columns.
	 *
	 * @return Row key as Map representation.
	 */
	public Map<String, Object> getRowKeyAsMap(RowKey rowKey) {
		Map<String, Object> map = new HashMap<String, Object>();

		if ( rowKey.getColumnNames() != null && rowKey.getColumnValues() != null ) {
			for ( int i = 0; i < rowKey.getColumnNames().length; i++ ) {
				map.put( rowKey.getColumnNames()[i], rowKey.getColumnValues()[i] );
			}
		}
		map.put( "table", rowKey.getTable() );
		return Collections.unmodifiableMap( map );
	}

	public Map<AssociationKey, Map<RowKey, Map<String, Object>>> getAssociationsMap() {
		Map<AssociationKey, Map<RowKey, Map<String, Object>>> associations = new HashMap<AssociationKey, Map<RowKey, Map<String, Object>>>();
		boolean isNull = false;
		Response<Map<String, String>> res = null;
		try {
			Transaction tx = jedis.multi();
			res = tx.hgetAll( ASSOCIATION_HSET );
			tx.exec();
		}
		catch (NullPointerException ex) {
			isNull = true;
		}
		catch (Exception ex) {
			throwHibernateExceptionFrom( ex );
		}
		finally {
			pool.returnResource( jedis );
		}

		if ( isNull ) {
			return Collections.EMPTY_MAP;
		}

		Entry<String, String> entry = null;
		for ( Iterator<Entry<String, String>> itr = res.get().entrySet().iterator(); itr.hasNext(); ) {
			entry = itr.next();
		}
		return associations;
	}

	public void removeAll() {
		log.info( "about to remove all data in Redis" );
		jedis = pool.getResource(); 
		try {
			jedis.flushDB();
		}
		finally {
			pool.returnResource( jedis );
		}
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
