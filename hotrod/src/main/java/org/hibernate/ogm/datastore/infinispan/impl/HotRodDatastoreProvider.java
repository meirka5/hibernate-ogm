/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2011-2013 Red Hat Inc. and/or its affiliates and other contributors
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
package org.hibernate.ogm.datastore.infinispan.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.HibernateException;
import org.hibernate.ogm.datastore.infinispan.impl.configuration.HotRodConfiguration;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.datastore.spi.DefaultDatastoreNames;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.dialect.infinispan.HotRodDialect;
import org.hibernate.ogm.options.navigation.context.GlobalContext;
import org.hibernate.ogm.options.navigation.impl.ConfigurationContext;
import org.hibernate.ogm.options.navigation.impl.GenericOptionModel;
import org.hibernate.ogm.service.impl.LuceneBasedQueryParserService;
import org.hibernate.ogm.service.impl.QueryParserService;
import org.hibernate.ogm.util.impl.Log;
import org.hibernate.ogm.util.impl.LoggerFactory;
import org.hibernate.service.jndi.spi.JndiService;
import org.hibernate.service.jta.platform.spi.JtaPlatform;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.util.FileLookupFactory;

/**
 * Provides access to Infinispan's CacheManager; one CacheManager is needed for all caches, it can be taken via JNDI or
 * started by this ServiceProvider; in this case it will also be stopped when no longer needed.
 *
 * @author Sanne Grinovero
 * @author Emmanuel Bernard <emmanuel@hibernate.org>
 */
public class HotRodDatastoreProvider implements DatastoreProvider, Startable, Stoppable, ServiceRegistryAwareService, Configurable {

	private static final Log log = LoggerFactory.make();

	private JtaPlatform jtaPlatform;
	private JndiService jndiService;
	private Map<String, RemoteCache> caches;
	private boolean isCacheProvided;
	private RemoteCacheManager cacheManager;
	private final HotRodConfiguration config = new HotRodConfiguration();

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return HotRodDialect.class;
	}

	@Override
	public Class<? extends QueryParserService> getDefaultQueryParserServiceType() {
		return LuceneBasedQueryParserService.class;
	}

	@Override
	public void start() {
		try {
			String jndiProperty = config.getJndiName();
			if ( jndiProperty == null ) {
				cacheManager = createCustomCacheManager( config.getConfigurationName(), jtaPlatform );
				isCacheProvided = false;
			}
			else {
				log.tracef( "Retrieving Infinispan from JNDI at %1$s", jndiProperty );
				cacheManager = (RemoteCacheManager) jndiService.locate( jndiProperty );
				isCacheProvided = true;
			}
		}
		catch (RuntimeException e) {
			throw log.unableToInitializeInfinispan( e );
		}
		eagerlyInitializeCaches( cacheManager );
		// clear resources
		this.jtaPlatform = null;
		this.jndiService = null;
	}

	/**
	 * Need to make sure all needed caches are started before state transfer happens. This prevents this node to return
	 * undefined cache errors during replication when other nodes join this one.
	 * 
	 * @param cacheManager
	 */
	private void eagerlyInitializeCaches(RemoteCacheManager cacheManager) {
		caches = new ConcurrentHashMap<String, RemoteCache>( 3 );
		putInLocalCache( cacheManager, DefaultDatastoreNames.ASSOCIATION_STORE );
		putInLocalCache( cacheManager, DefaultDatastoreNames.ENTITY_STORE );
		putInLocalCache( cacheManager, DefaultDatastoreNames.IDENTIFIER_STORE );
	}

	private void putInLocalCache(RemoteCacheManager cacheManager, String cacheName) {
		caches.put( cacheName, cacheManager.getCache( cacheName ) );
	}

	private RemoteCacheManager createCustomCacheManager(String cfgName, JtaPlatform platform) {
		TransactionManagerLookupDelegator transactionManagerLookupDelegator = new TransactionManagerLookupDelegator( platform );
		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			InputStream configurationFile = FileLookupFactory.newInstance().lookupFileStrict( cfgName, contextClassLoader );
			try {
				return new RemoteCacheManager( configuration( configurationFile ) );
			}
			finally {
				if ( configurationFile != null ) {
					configurationFile.close();
				}
			}
		}
		catch (RuntimeException re) {
			throw raiseConfigurationError( re, cfgName );
		}
		catch (IOException e) {
			throw raiseConfigurationError( e, cfgName );
		}
	}

	private Configuration configuration(InputStream configurationFile) throws IOException {
		ConfigurationBuilder builder = new ConfigurationBuilder().withProperties( properties( configurationFile ) );
		Configuration configuration = builder.create();
		return configuration;
	}

	private Properties properties(InputStream configurationFile) throws IOException {
		Properties properties = new Properties();
		properties.load( configurationFile );
		return properties;
	}

	public RemoteCacheManager getEmbeddedCacheManager() {
		return cacheManager;
	}

	// prefer generic form over specific ones to prepare for flexible cache setting
	public RemoteCache getCache(String name) {
		return caches.get( name );
	}

	public void clearCaches() {
		for ( RemoteCache cache : caches.values() ) {
			cache.clear();
		}
	}

	@Override
	public void stop() {
		if ( !isCacheProvided && cacheManager != null ) {
			cacheManager.stop();
		}
	}

	private HibernateException raiseConfigurationError(Exception e, String cfgName) {
		return new HibernateException( "Could not start Infinispan CacheManager using as configuration file: " + cfgName, e );
	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		jtaPlatform = serviceRegistry.getService( JtaPlatform.class );
		jndiService = serviceRegistry.getService( JndiService.class );
	}

	@Override
	public void configure(Map configurationValues) {
		this.config.initConfiguration( configurationValues );
	}

	@Override
	public GlobalContext<?, ?> getConfigurationBuilder(ConfigurationContext context) {
		return GenericOptionModel.createGlobalContext( context );
	}
}
