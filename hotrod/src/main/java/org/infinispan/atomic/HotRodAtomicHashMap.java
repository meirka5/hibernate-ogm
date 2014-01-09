package org.infinispan.atomic;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

/**
 * The default implementation of {@link AtomicMap}. Note that this map cannot be constructed directly, and callers
 * should obtain references to AtomicHashMaps via the {@link AtomicMapLookup} helper. This helper will ensure proper
 * concurrent construction and registration of AtomicMaps in Infinispan's data container. E.g.: <br />
 * <br />
 * <code>
 *    AtomicMap&lt;String, Integer&gt; map = AtomicMapLookup.getAtomicMap(cache, "my_atomic_map_key");
 * </code> <br />
 * <br />
 * Note that for replication to work properly, AtomicHashMap updates <b><i>must always</i></b> take place within the
 * scope of an ongoing JTA transaction or batch (see {@link Cache#startBatch()}).
 * <p/>
 *
 * @author (various)
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 * @see AtomicMap
 * @see AtomicMapLookup
 * @since 4.0
 */
public final class HotRodAtomicHashMap<K, V> implements AtomicMap<K, V>, DeltaAware, Cloneable {

	private AtomicHashMapDelta delta = null;
	private volatile HotRodAtomicHashMapProxy<K, V> proxy;
	volatile boolean copied = false;
	volatile boolean removed = false;
	private final RemoteCacheImpl<K, V> delegate;

	public HotRodAtomicHashMap(RemoteCacheManager rcm, String name) {
		this( rcm, name, false );
	}

	public HotRodAtomicHashMap(RemoteCacheManager rcm, String name, boolean b) {
		this.delegate = new RemoteCacheImpl<K, V>( rcm, name );
		this.copied = b;
	}

	public HotRodAtomicHashMap(RemoteCacheImpl<K, V> delegate, boolean b) {
		this.delegate = delegate;
		this.copied = b;
	}

	public HotRodAtomicHashMap(RemoteCacheImpl<K, V> delegate, HotRodAtomicHashMapProxy<K, V> proxy) {
		this.delegate = delegate;
		this.proxy = proxy;
	}

	/**
	 * Construction only allowed through this factory method. This factory is intended for use internally by the
	 * CacheDelegate.
	 */
	public static <K, V> HotRodAtomicHashMap<K, V> newInstance( RemoteCacheManager rcm, RemoteCache<Object, Object> cache, Object cacheKey) {
		HotRodAtomicHashMap<K, V> value = new HotRodAtomicHashMap<K, V>(rcm, "testing");
		Object oldValue = cache.putIfAbsent(cacheKey, value);
		if (oldValue != null) {
			value = (HotRodAtomicHashMap<K, V>) oldValue;
		}
		return value;
	}

//	private HotRodAtomicHashMap(RemoteCacheImpl<K, V> newDelegate, HotRodAtomicHashMapProxy<K, V> proxy) {
//		this.delegate = newDelegate;
//		this.proxy = proxy;
//		this.copied = true;
//	}
//
//	private HotRodAtomicHashMap(RemoteCache<K, V> newDelegate) {
//		this.delegate = newDelegate;
//		this.copied = true;
//	}

	@Override
	public void commit() {
		copied = false;
		delta = null;
	}

	@Override
	public V put(K key, V value) {
		V oldValue = delegate.put( key, value );
		getDelta().addOperation( new org.infinispan.atomic.PutOperation( key, oldValue, value ) );
		return oldValue;
	}

	@Override
	@SuppressWarnings("unchecked")
	public V remove(Object key) {
		V oldValue = delegate.remove( key );
		RemoveOperation<K, V> op = new RemoveOperation<K, V>( (K) key, oldValue );
		getDelta().addOperation( op );
		return oldValue;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> t) {
		// this is crappy - need to do this more efficiently!
		for ( Entry<? extends K, ? extends V> e : t.entrySet() ) {
			put( e.getKey(), e.getValue() );
		}
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Builds a thread-safe proxy for this instance so that concurrent reads are isolated from writes.
	 * @param rcm 
	 * 
	 * @return an instance of AtomicHashMapProxy
	 */
	HotRodAtomicHashMapProxy<K, V> getProxy(RemoteCacheManager rcm, RemoteCache<Object, AtomicMap<K, V>> cache, Object mapKey, boolean fineGrained) {
		// construct the proxy lazily
		if ( proxy == null ) // DCL is OK here since proxy is volatile (and we live in a post-JDK 5 world)
		{
			synchronized ( this ) {
				if ( proxy == null ) {
					proxy = new HotRodAtomicHashMapProxy<K, V>( cache, mapKey );
				}
			}
		}
		return proxy;
	}

	public void markRemoved(boolean b) {
		removed = b;
	}

	@Override
	public Delta delta() {
		Delta toReturn = delta == null ? NullDelta.INSTANCE : delta;
		delta = null; // reset
		return toReturn;
	}

	@SuppressWarnings("unchecked")
	public HotRodAtomicHashMap<K, V> copy() {
		return new HotRodAtomicHashMap<K, V>( delegate, proxy );
	}

	@Override
	public String toString() {
		// Sanne: Avoid iterating on the delegate as that might lead to
		// exceptions from concurrent iterators: not nice to have during a toString!
		//
		// Galder: Sure, but we need a way to track the contents of the atomic
		// hash map somehow, so, we need to log each operation that affects its
		// contents, and when its state is restored.
		return "HotRodAtomicHashMap";
	}

	/**
	 * Initializes the delta instance to start recording changes.
	 */
	public void initForWriting() {
		delta = new AtomicHashMapDelta();
	}

	AtomicHashMapDelta getDelta() {
		if ( delta == null )
			delta = new AtomicHashMapDelta();
		return delta;
	}

	@Override
	public int size() {
		return delegate.size();
	}

	@Override
	public boolean isEmpty() {
		return delegate.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return delegate.containsKey( key );
	}

	@Override
	public boolean containsValue(Object value) {
		return delegate.containsValue( value );
	}

	@Override
	public V get(Object key) {
		return delegate.get( key );
	}

	@Override
	public Set<K> keySet() {
		return delegate.keySet();
	}

	@Override
	public Collection<V> values() {
		return delegate.values();
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return delegate.entrySet();
	}

	public static class Externalizer extends AbstractExternalizer<HotRodAtomicHashMap> {

		@Override
		public void writeObject(ObjectOutput output, HotRodAtomicHashMap map) throws IOException {
			output.writeObject( map.delegate.getName() );
			output.writeObject( map.delegate.getRemoteCacheManager() );
		}

		@Override
		@SuppressWarnings("unchecked")
		public HotRodAtomicHashMap readObject(ObjectInput input) throws IOException, ClassNotFoundException {
			String name = (String) input.readObject();
			RemoteCacheManager rcm = (RemoteCacheManager) input.readObject();
			return new HotRodAtomicHashMap( rcm, name );
		}

		@Override
		public Integer getId() {
			return Ids.ATOMIC_HASH_MAP;
		}

		@Override
		@SuppressWarnings("unchecked")
		public Set<Class<? extends HotRodAtomicHashMap>> getTypeClasses() {
			return Util.<Class<? extends HotRodAtomicHashMap>> asSet( HotRodAtomicHashMap.class );
		}
	}

}
