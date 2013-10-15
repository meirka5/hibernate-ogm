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
package org.hibernate.ogm.dialect.neo4j;

import java.util.HashMap;
import java.util.Map;

import org.hibernate.ogm.datastore.neo4j.impl.Neo4jDatastoreProvider;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.RowKey;
import org.neo4j.graphdb.RelationshipType;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * Manages {@link Vertex} and {@link Edge} indexes.
 *
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class Neo4jIndexManager {

	private static final String TABLE_PROPERTY = "_table";
	private static final String RELATIONSHIP_TYPE = "_relationship_type";

	private final Neo4jDatastoreProvider provider;

	public Neo4jIndexManager(Neo4jDatastoreProvider provider) {
		this.provider = provider;
	}

	/**
	 * Index a {@link Vertex}.
	 *
	 * @see Neo4jIndexManager#findNode(EntityKey)
	 * @param node
	 *            the Vertex to index
	 * @param entityKey
	 *            the {@link EntityKey} representing the node
	 */
	public void index(Vertex node, EntityKey entityKey) {
		Index<Vertex> nodeIndex = provider.getNodesIndex();
		nodeIndex.put( TABLE_PROPERTY, entityKey.getTable(), node );
		for ( int i = 0; i < entityKey.getColumnNames().length; i++ ) {
			nodeIndex.put( entityKey.getColumnNames()[i], entityKey.getColumnValues()[i], node );
		}
	}

	private String escape(String name) {
		if ( StringFactory.ID.equals( name ) ) {
			name = "<" + name + ">";
		}
		return name;
	}

	private Map<String, Object> nodeProperties(EntityKey entitykey) {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put( TABLE_PROPERTY, entitykey.getTable() );
		for ( int j = 0; j < entitykey.getColumnNames().length; j++ ) {
			properties.put( entitykey.getColumnNames()[j], entitykey.getColumnValues()[j] );
		}
		return properties;
	}

	/**
	 * Index a {@link Edge}.
	 *
	 * @see Neo4jIndexManager#findRelationship(RelationshipType, RowKey)
	 * @param relationship
	 *            the Edge to index
	 */
	public void index(Edge relationship) {
		Index<Edge> relationshipIndex = provider.getRelationshipsIndex();
		relationshipIndex.put( RELATIONSHIP_TYPE, relationship.getLabel(), relationship );
		for ( String key : relationship.getPropertyKeys() ) {
			relationshipIndex.put( key, relationship.getProperty( key ), relationship );
		}
	}

	/**
	 * Looks for a {@link Edge} in the index.
	 *
	 * @param type
	 *            the {@link RelationshipType} of the wanted relationship
	 * @param rowKey
	 *            the {@link RowKey} that representing the relationship.
	 * @return the relationship found or null
	 */
	public Edge findRelationship(RelationshipType type, RowKey rowKey) {
		String query = createQuery( properties( type, rowKey ) );
		Index<Edge> relationshipIndex = provider.getRelationshipsIndex();
		CloseableIterable<Edge> iterator = relationshipIndex.query( query, Edge.class );
		if ( iterator.iterator().hasNext() ) {
			Edge next = iterator.iterator().next();
			iterator.close();
			return next;
		}
		return null;
	}

	private Map<String, Object> properties(RelationshipType type, RowKey rowKey) {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put( RELATIONSHIP_TYPE, type.name() );
		for ( int i = 0; i < rowKey.getColumnNames().length; i++ ) {
			properties.put( rowKey.getColumnNames()[i], rowKey.getColumnValues()[i] );
		}
		return properties;
	}

	/**
	 * Looks for a {@link Vertex} in the index.
	 *
	 * @param entityKey
	 *            the {@link EntityKey} that identify the node.
	 * @return the node found or null
	 */
	public Vertex findNode(EntityKey entityKey) {
		String query = createQuery( nodeProperties( entityKey ) );
		Index<Vertex> nodeIndex = provider.getNodesIndex();
		CloseableIterable<Vertex> iterator = nodeIndex.query( query, Vertex.class );
		if ( iterator.iterator().hasNext() ) {
			Vertex next = iterator.iterator().next();
			iterator.close();
			return next;
		}
		return null;
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
	 * @param rel
	 *            the Edge is going to be removed from the index
	 *
	 */
	public void remove(Edge rel) {
		Index<Edge> relationshipIndex = provider.getRelationshipsIndex();
		for ( String key : rel.getPropertyKeys() ) {
			relationshipIndex.remove( key, rel.getProperty( key ), rel );
		}
	}

	public void remove(Vertex entityNode) {
		Index<Vertex> nodeIndex = provider.getNodesIndex();
		for ( String key : entityNode.getPropertyKeys() ) {
			nodeIndex.remove( key, entityNode.getProperty( key ), entityNode );
		}
	}

	/**
	 * Return all the indexed nodes  corresponding to an entity type.
	 *
	 * @param tableName
	 *            the name of the table representing the entity
	 * @return the nodes representing the entities
	 */
	public CloseableIterable<Vertex> findNodes(String tableName) {
		Index<Vertex> nodeIndex = provider.getNodesIndex();
		return nodeIndex.get( TABLE_PROPERTY, tableName );
	}

}
