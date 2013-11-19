package org.hibernate.ogm.dialect.hotrod.atomic;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.atomic.Operation;
import org.infinispan.client.hotrod.impl.operations.AbstractKeyOperation;
import org.infinispan.client.hotrod.impl.operations.ClearOperation;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Changes that have occurred on an AtomicHashMap
 * 
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @since 4.0
 */
public class HotRodAtomicHashMapDelta implements Delta {

	private static final Log log = LogFactory.getLog( HotRodAtomicHashMapDelta.class );
	private static final boolean trace = log.isTraceEnabled();

	private List<HotRodOperation> changeLogs;
	private boolean hasClearOperation;

	@Override
	public DeltaAware merge(DeltaAware deltaAware) {
		RemoteAtomicCache<Object, Object> other = other( deltaAware );
		if ( changeLogs != null ) {
			for ( AbstractKeyOperation changeLog : changeLogs ) {
				other.put( key, value )
				changeLog.replay( other.delegate );
			}
		}
		return other;
	}

	private RemoteAtomicCache<Object, Object> other(DeltaAware deltaAware) {
		HotRodAtomicHashMap<Object, Object> other;
		if ( deltaAware != null && ( deltaAware instanceof HotRodAtomicHashMap ) ) {
			other = (HotRodAtomicHashMap<Object, Object>) deltaAware;
		}
		else {
			other = new HotRodAtomicHashMap();
		}
		return other;
	}

	public void addOperation(HotRodOperation o) {
		if ( changeLogs == null ) {
			// lazy init
			changeLogs = new LinkedList<AbstractKeyOperation>();
		}
		if ( trace )
			log.tracef( "Add operation %s to delta", o );

		changeLogs.add( o );
	}

	public Collection<Object> getKeys() {
		List<Object> keys = new LinkedList<Object>();
		if ( changeLogs != null ) {
			for ( AbstractKeyOperation o : changeLogs ) {
				Object key = o.keyAffected();
				keys.add( key );
			}
		}
		return keys;
	}

	public boolean hasClearOperation() {
		return hasClearOperation;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder( "AtomicHashMapDelta{changeLog=" );
		sb.append( changeLogs );
		sb.append( ",hasClear=" );
		sb.append( hasClearOperation );
		sb.append( "}" );
		return sb.toString();
	}

	public int getChangeLogSize() {
		return changeLogs == null ? 0 : changeLogs.size();
	}

	public static class Externalizer extends AbstractExternalizer<HotRodAtomicHashMapDelta> {

		@Override
		public void writeObject(ObjectOutput output, HotRodAtomicHashMapDelta delta) throws IOException {
			if ( trace )
				log.tracef( "Serializing changeLog %s", delta.changeLogs );
			output.writeObject( delta.changeLogs );
		}

		@Override
		public HotRodAtomicHashMapDelta readObject(ObjectInput input) throws IOException, ClassNotFoundException {
			HotRodAtomicHashMapDelta delta = new HotRodAtomicHashMapDelta();
			delta.changeLogs = (List<HotRodOperation>) input.readObject();
			if ( trace )
				log.tracef( "Deserialized changeLog %s", delta.changeLogs );
			return delta;
		}

		@Override
		public Integer getId() {
			return Ids.ATOMIC_HASH_MAP_DELTA;
		}

		@Override
		public Set<Class<? extends HotRodAtomicHashMapDelta>> getTypeClasses() {
			return Util.<Class<? extends HotRodAtomicHashMapDelta>> asSet( HotRodAtomicHashMapDelta.class );
		}
	}
}
