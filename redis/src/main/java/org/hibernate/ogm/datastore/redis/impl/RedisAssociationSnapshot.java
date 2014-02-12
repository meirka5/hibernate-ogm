/**
 * 
 */
package org.hibernate.ogm.datastore.redis.impl;

import java.util.Map;
import java.util.Set;

import org.hibernate.ogm.datastore.spi.AssociationSnapshot;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.grid.RowKey;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 *
 */
public class RedisAssociationSnapshot implements AssociationSnapshot {

	private final Map<RowKey, Map<String, String>> associationMap;

	public RedisAssociationSnapshot(Map<RowKey, Map<String, String>> associationMap) {
		this.associationMap = associationMap;
	}

	@Override
	public Tuple get(RowKey column) {
		Map<String, String> rawResult = associationMap.get( column );
		return rawResult != null ? new Tuple( new RedisTupleSnapshot( rawResult ) ) : null;
	}

	@Override
	public boolean containsKey(RowKey column) {
		return associationMap.containsKey( column );
	}

	@Override
	public int size() {
		return associationMap.size();
	}

	@Override
	public Set<RowKey> getRowKeys() {
		return associationMap.keySet();
	}

	public Map<RowKey, Map<String, String>> getUnderlyingMap() {
		return associationMap;
	}

}
