/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.remote.dialect.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.hibernate.ogm.datastore.neo4j.Neo4jDialect;
import org.hibernate.ogm.datastore.neo4j.remote.impl.Neo4jClient;
import org.hibernate.ogm.datastore.neo4j.remote.json.impl.Graph.Node;
import org.hibernate.ogm.model.key.spi.AssociatedEntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.spi.TupleSnapshot;
import org.hibernate.ogm.persister.impl.OgmEntityPersister;

/**
 * Represents the Tuple snapshot as loaded by the Neo4j datastore.
 * <p>
 * The columns of the tuple are mapped as properties of the node.
 *
 * @author Davide D'Alto &lt;davide@hibernate.org&gt;
 */
public final class RemoteNeo4jTupleSnapshot implements TupleSnapshot {

	private final Node node;
	private final Map<String, AssociatedEntityKeyMetadata> associatedEntityKeyMetadata;
	private final Map<String, String> rolesByColumn;
	private final EntityKeyMetadata entityKeyMetadata;
	private final Neo4jClient neo4jClient;

	private final Map<String, Node> toOneEntities;
	private final RemoteNeo4jEntityQueries queries;

	public RemoteNeo4jTupleSnapshot(Neo4jClient neo4jClient, RemoteNeo4jEntityQueries queries, Node node, EntityKeyMetadata entityKeyMetadata, OgmEntityPersister ogmEntityPersister) {
		this( neo4jClient, queries, node, Collections.<String, AssociatedEntityKeyMetadata>emptyMap(), Collections.<String, String>emptyMap(), entityKeyMetadata, ogmEntityPersister );
	}

	public RemoteNeo4jTupleSnapshot(Neo4jClient neo4jClient, RemoteNeo4jEntityQueries queries, Node node, Map<String, AssociatedEntityKeyMetadata> associatedEntityKeyMetadata, Map<String, String> rolesByColumn, EntityKeyMetadata entityKeyMetadata) {
		this( neo4jClient, queries, node, associatedEntityKeyMetadata, rolesByColumn, entityKeyMetadata, null );
	}

	private RemoteNeo4jTupleSnapshot(Neo4jClient neo4jClient, RemoteNeo4jEntityQueries queries, Node node,
			Map<String, AssociatedEntityKeyMetadata> associatedEntityKeyMetadata, Map<String, String> rolesByColumn, EntityKeyMetadata entityKeyMetadata,
			OgmEntityPersister ogmEntityPersister) {
		this.neo4jClient = neo4jClient;
		this.queries = queries;
		this.node = node;
		this.associatedEntityKeyMetadata = associatedEntityKeyMetadata;
		this.rolesByColumn = rolesByColumn;
		this.entityKeyMetadata = entityKeyMetadata;
		if ( associatedEntityKeyMetadata.size() == 0 ) {
			this.toOneEntities = Collections.emptyMap();
		}
		else {
			this.toOneEntities = new HashMap<>( associatedEntityKeyMetadata.size() );
		}
	}

	@Override
	public Object get(String column) {
		return getColumn( column );
	}

	private Object getColumn(String column) {
		if ( associatedEntityKeyMetadata.containsKey( column ) ) {
			return readPropertyOnOtherNode( column );
		}
		else if ( Neo4jDialect.isPartOfRegularEmbedded( entityKeyMetadata.getColumnNames(), column ) ) {
			return readEmbeddedProperty( column );
		}
		else {
			return readProperty( node, column );
		}
	}

	private Object readPropertyOnOtherNode(String column) {
		String associationrole = rolesByColumn.get( column );
		Node associatedEntity = toOneEntities.get( associationrole );
		if ( associatedEntity == null ) {
			// Not cached, let's look for it
			associatedEntity = queries.findAssociatedEntity( neo4jClient, keyValues(), associationrole );
			if ( associatedEntity == null ) {
				return null;
			}
			else {
				toOneEntities.put( associationrole, associatedEntity );
			}
		}

		return readProperty( associatedEntity, associatedEntityKeyMetadata.get( column ).getCorrespondingEntityKeyColumn( column ) );
	}

	private Object[] keyValues() {
		Object[] values = new Object[entityKeyMetadata.getColumnNames().length];
		for ( int i = 0; i < values.length; i++ ) {
			values[i] = node.getProperties().get( entityKeyMetadata.getColumnNames()[i] );
		}
		return values;
	}

	private Object readEmbeddedProperty(String column) {
		String embeddedPath = column.substring( 0, column.lastIndexOf( "." ) );
		Object[] keyValues = keyValues();
		Node embeddedNode = queries.findEmbeddedNode( neo4jClient, keyValues, embeddedPath );
		if ( embeddedNode == null ) {
			return null;
		}

		String columnName = column.substring( column.lastIndexOf( "." ) + 1 );
		return readProperty( embeddedNode, columnName );
	}

	private Object readProperty(Node node, String targetColumnName) {
		Object value = null;
		if ( node.getProperties().containsKey( targetColumnName ) ) {
			value = node.getProperties().get( targetColumnName );
		}
		return value;
	}

	@Override
	public boolean isEmpty() {
		return node.getProperties().isEmpty();
	}

	@Override
	public Set<String> getColumnNames() {
		Set<String> names = new HashSet<String>();
		for ( String string : node.getProperties().keySet() ) {
			names.add( string );
		}
		return names;
	}

	public Node getNode() {
		return node;
	}

}
