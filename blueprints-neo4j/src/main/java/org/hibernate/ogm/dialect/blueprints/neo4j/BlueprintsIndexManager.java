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
package org.hibernate.ogm.dialect.blueprints.neo4j;

import java.util.HashMap;
import java.util.Map;

import org.hibernate.ogm.datastore.blueprints.neo4j.impl.Neo4jBlueprintsDatastoreProvider;
import org.hibernate.ogm.datastore.blueprints.neo4j.impl.ReservedWordWrapper;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.RowKey;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;

/**
 * Manages {@link Vertex} and {@link Edge} indexes.
 *
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class BlueprintsIndexManager {

	public static final String RELATIONSHIP_TYPE = "_relationship_type";

	private static final String TABLE_PROPERTY = BlueprintsDialect.TABLE_PROPERTY;

	private final Neo4jBlueprintsDatastoreProvider provider;

	public BlueprintsIndexManager(Neo4jBlueprintsDatastoreProvider provider) {
		this.provider = provider;
	}

	/**
	 * Index a {@link Vertex}.
	 *
	 * @see BlueprintsIndexManager#findVertex(EntityKey)
	 * @param vertex the Vertex to index
	 * @param key the {@link EntityKey} representing the vertex
	 */
	public void index(EntityKey key, Vertex vertex) {
		Index<Vertex> vertexIndex = provider.getVertexesIndex();
		vertexIndex.put( TABLE_PROPERTY, key.getTable(), vertex );
		for ( int i = 0; i < key.getColumnNames().length; i++ ) {
			vertexIndex.put( key.getColumnNames()[i], key.getColumnValues()[i], vertex );
		}
	}

	/**
	 * Index a {@link Edge}.
	 *
	 * @param edge the Edge to index
	 */
	public void index(Edge edge) {
		Index<Edge> relationshipIndex = provider.getEdgesIndex();
		relationshipIndex.put( RELATIONSHIP_TYPE, edge.getLabel(), edge );
		for ( String key : edge.getPropertyKeys() ) {
			relationshipIndex.put( key, edge.getProperty( key ), edge );
		}
	}

	/**
	 * Looks for a {@link Edge} in the index.
	 *
	 * @param label the label of the wanted relationship
	 * @param rowKey the {@link RowKey} that representing the relationship.
	 * @return the relationship found or null
	 */
	public Edge findRelationship(RowKey rowKey, String label) {
		String query = createQuery( properties( label, rowKey ) );
		Index<Edge> relationshipIndex = provider.getEdgesIndex();
		CloseableIterable<Edge> iterator = relationshipIndex.query( null, query );
		if ( iterator.iterator().hasNext() ) {
			Edge next = iterator.iterator().next();
			iterator.close();
			return next;
		}
		return null;
	}

	private Map<String, Object> properties(String label, RowKey rowKey) {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put( RELATIONSHIP_TYPE, label );
		for ( int i = 0; i < rowKey.getColumnNames().length; i++ ) {
			properties.put( ReservedWordWrapper.escape( rowKey.getColumnNames()[i] ), rowKey.getColumnValues()[i] );
		}
		return properties;
	}

	/**
	 * Looks for a {@link Vertex} in the index.
	 *
	 * @param entityKey the {@link EntityKey} that identify the vertex.
	 * @return the vertex found or null
	 */
	public Vertex findVertex(EntityKey entityKey) {
		String query = createQuery( vertexProperties( entityKey ) );
		Index<Vertex> vertexIndex = provider.getVertexesIndex();
		CloseableIterable<Vertex> iterator = vertexIndex.query( null, query );
		if ( iterator.iterator().hasNext() ) {
			Vertex next = iterator.iterator().next();
			iterator.close();
			return next;
		}
		return null;
	}

	private Map<String, Object> vertexProperties(EntityKey entitykey) {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put( TABLE_PROPERTY, entitykey.getTable() );
		for ( int j = 0; j < entitykey.getColumnNames().length; j++ ) {
			properties.put( entitykey.getColumnNames()[j], entitykey.getColumnValues()[j] );
		}
		return properties;
	}

	private String createQuery(Map<String, Object> properties) {
		StringBuilder queryBuilder = new StringBuilder();
		for ( Map.Entry<String, Object> entry : properties.entrySet() ) {
			queryBuilder.append( " AND " );
			appendTerm( queryBuilder, entry.getKey(), entry.getValue() );
		}
		return queryBuilder.substring( " AND ".length() );
	}

	private void appendTerm(StringBuilder queryBuilder, String key, Object value) {
		queryBuilder.append( key );
		queryBuilder.append( ": \"" );
		queryBuilder.append( value );
		queryBuilder.append( "\"" );
	}

	/**
	 * Remove a {@link Edge} from the index.
	 *
	 * @param edge the Edge that is going to be removed from the index
	 */
	public void remove(Edge edge) {
		Index<Edge> relationshipIndex = provider.getEdgesIndex();
		relationshipIndex.remove( RELATIONSHIP_TYPE, edge.getLabel(), edge );
		for ( String key : edge.getPropertyKeys() ) {
			relationshipIndex.put( key, edge.getProperty( key ), edge );
		}
	}

	public void remove(EntityKey key, Vertex vertex) {
		Index<Vertex> vertexIndex = provider.getVertexesIndex();
		vertexIndex.remove( TABLE_PROPERTY, vertex.getProperty( TABLE_PROPERTY ), vertex );
		for ( int i = 0; i < key.getColumnNames().length; i++ ) {
			vertexIndex.remove( key.getColumnNames()[i], key.getColumnValues()[i], vertex );
		}
	}

	/**
	 * Return all the indexed vertexes corresponding to an entity type.
	 *
	 * @param tableName the name of the table representing the entity
	 * @return the vertexes representing the entities
	 */
	public CloseableIterable<Vertex> findVertexes(String tableName) {
		Index<Vertex> vertexIndex = provider.getVertexesIndex();
		return vertexIndex.query( null, TABLE_PROPERTY + ":\"" + tableName + "\"" );
	}

}
