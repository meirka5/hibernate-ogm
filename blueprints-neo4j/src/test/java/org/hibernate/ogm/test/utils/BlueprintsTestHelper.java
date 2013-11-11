/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.fest.util.Files;
import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.datastore.blueprints.neo4j.impl.Neo4jBlueprintsDatastoreProvider;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.datastore.spi.TupleSnapshot;
import org.hibernate.ogm.dialect.blueprints.neo4j.BlueprintsDialect;
import org.hibernate.ogm.dialect.blueprints.neo4j.BlueprintsIndexManager;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.util.impl.Log;
import org.hibernate.ogm.util.impl.LoggerFactory;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class BlueprintsTestHelper implements TestableGridDialect {

	private final static Log log = LoggerFactory.make();

	@Override
	public boolean assertNumberOfEntities(int numberOfEntities, SessionFactory sessionFactory) {
		return numberOfEntities == countEntities( sessionFactory );
	}

	@Override
	public boolean assertNumberOfAssociations(int numberOfAssociations, SessionFactory sessionFactory) {
		return numberOfAssociations == countAssociations( sessionFactory );
	}

	@Override
	public Map<String, Object> extractEntityTuple(SessionFactory sessionFactory, EntityKey key) {
		Map<String, Object> tuple = new HashMap<String, Object>();
		BlueprintsDialect dialect = new BlueprintsDialect( getProvider( sessionFactory ) );
		TupleSnapshot snapshot = dialect.getTuple( key, null ).getSnapshot();
		for ( String column : snapshot.getColumnNames() ) {
			tuple.put( column, snapshot.get( column ) );
		}
		return tuple;
	}

	@Override
	public boolean backendSupportsTransactions() {
		return false;
	}

	@Override
	public void dropSchemaAndDatabase(SessionFactory sessionFactory) {
		getProvider( sessionFactory ).stop();
		Files.delete( new File( dbLocation() ) );
	}

	private String dbLocation() {
		InputStream inStream = this.getClass().getClassLoader().getResourceAsStream( "blueprints.properties" );
		Properties properties = new Properties();
		try {
			properties.load( inStream );
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		String location = properties.getProperty( "blueprints.neo4j.directory" );
		return location;
	}

	@Override
	public Map<String, String> getEnvironmentProperties() {
		Map<String, String> properties = new HashMap<String, String>();
		return properties;
	}

	private static Neo4jBlueprintsDatastoreProvider getProvider(SessionFactory sessionFactory) {
		DatastoreProvider provider = ( (SessionFactoryImplementor) sessionFactory ).getServiceRegistry().getService( DatastoreProvider.class );
		if ( !( Neo4jBlueprintsDatastoreProvider.class.isInstance( provider ) ) ) {
			throw new RuntimeException( "Not testing with Neo4jBlueprints, cannot extract underlying provider" );
		}
		return Neo4jBlueprintsDatastoreProvider.class.cast( provider );
	}

	public int countAssociations(SessionFactory sessionFactory) {
		String allLabelsQuery = BlueprintsIndexManager.RELATIONSHIP_TYPE + ":*";
		CloseableIterable<Edge> relationships = getProvider( sessionFactory ).getEdgesIndex().query( null, allLabelsQuery );
		Iterator<Edge> iterator = relationships.iterator();
		int count = 0;
		while ( iterator.hasNext() ) {
			iterator.next();
			count++;
		}
		relationships.close();
		return count;
	}

	public int countEntities(SessionFactory sessionFactory) {
		String allEntitiesQuery = BlueprintsDialect.TABLE_PROPERTY + ":*";
		CloseableIterable<Vertex> nodes = getProvider( sessionFactory ).getVertexesIndex().query( null, allEntitiesQuery );
		int count = 0;
		Iterator<Vertex> iterator = nodes.iterator();
		while ( iterator.hasNext() ) {
			iterator.next();
			count++;
		}
		nodes.close();
		return count;
	}

}
