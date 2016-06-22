/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.remote.dialect.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.hibernate.ogm.datastore.neo4j.remote.json.impl.Graph.Node;

/**
 * An entity node and all the embedded values associated to it.
 *
 * @author Davide D'Alto
 */
public class NodeWithEmbeddeds {

	private static final Map<String, Collection<Node>> EMPTY_MAP = Collections.<String, Collection<Node>>emptyMap();

	private final Node owner;
	private final Map<String, Collection<Node>> embeddedNodes;

	public NodeWithEmbeddeds(Node owner) {
		this( owner, EMPTY_MAP );
	}

	public NodeWithEmbeddeds(Node owner, Map<String, Collection<Node>> embeddedNodes) {
		this.owner = owner;
		this.embeddedNodes = embeddedNodes == null ? EMPTY_MAP : Collections.unmodifiableMap( embeddedNodes );
	}

	public Node getOwner() {
		return owner;
	}

	/**
	 * The set of Nodes associated to a specific path.
	 * @return
	 */
	public Map<String, Collection<Node>> getEmbeddedNodes() {
		return embeddedNodes;
	}
}
