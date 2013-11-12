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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.datastore.blueprints.neo4j.impl.Neo4jBlueprintsDatastoreProvider;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.datastore.spi.TupleSnapshot;
import org.hibernate.ogm.dialect.blueprints.neo4j.BlueprintsDialect;
import org.hibernate.ogm.dialect.blueprints.neo4j.BlueprintsIndexManager;
import org.hibernate.ogm.grid.EntityKey;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class BlueprintsTestHelper implements TestableGridDialect {

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
//		Files.delete( new File( dbLocation() ) );
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
		properties.put( "blueprints.neo4j.config.org.neo4j.server.database.location", dbLocation() + "/prova/graph.db" );
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
		String allEdgeTypes = BlueprintsIndexManager.RELATIONSHIP_TYPE + ":*";
		CloseableIterable<Edge> relationships = getProvider( sessionFactory ).getEdgesIndex().query( null, allEdgeTypes );
		Set<String> uniqueAssociationTypes = new HashSet<String>();
		Iterator<Edge> iterator = relationships.iterator();
		while ( iterator.hasNext() ) {
			Edge relationship = (Edge) iterator.next();
			if ( !uniqueAssociationTypes.contains( relationship.getLabel() ) ) {
				uniqueAssociationTypes.add( relationship.getLabel() );
			}
		}
		relationships.close();
		return uniqueAssociationTypes.size();
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
