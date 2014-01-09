package org.infinispan.atomic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.container.entries.DeltaAwareCacheEntry;

/**
 * A layer of indirection around an {@link FineGrainedAtomicMap} to provide consistency and isolation for concurrent readers
 * while writes may also be going on.  The techniques used in this implementation are very similar to the lock-free
 * reader MVCC model used in the {@link org.infinispan.container.entries.MVCCEntry} implementations for the core data
 * container, which closely follow software transactional memory approaches to dealing with concurrency.
 * <br /><br />
 * Typically proxies are only created by the {@link AtomicMapLookup} helper, and would not be created by end-user code
 * directly.
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Sanne Grinovero
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 * @see HotRodAtomicHashMap
 * @since 5.1
 */
public class HotRodFineGrainedAtomicHashMapProxy<K, V> extends HotRodAtomicHashMapProxy<K, V> implements FineGrainedAtomicMap<K,V> {

   HotRodFineGrainedAtomicHashMapProxy(RemoteCache<Object, AtomicMap<K, V>> cache, Object deltaMapKey) {
     super(cache, deltaMapKey);
   }

   @SuppressWarnings("unchecked")
   @Override
   protected HotRodAtomicHashMap<K, V> getDeltaMapForWrite() {
//      CacheEntry lookedUpEntry = lookupEntryFromCurrentTransaction();
//      boolean lockedAndCopied = lookedUpEntry != null && lookedUpEntry.isChanged() &&
//            toMap(lookedUpEntry.getValue()).copied;
//
//      if (lockedAndCopied) {
//         return getDeltaMapForRead();
//      } else {
         HotRodAtomicHashMap<K, V> map = getDeltaMapForRead();
         boolean insertNewMap = map == null;
         // copy for write
         HotRodAtomicHashMap<K, V> copy = insertNewMap ? new HotRodAtomicHashMap<K, V>(true) : map.copy();
         copy.initForWriting();
         if (insertNewMap) {
            cacheForWriting.put(deltaMapKey, copy);
         }
         return copy;
//      }
   }

   @Override
   public Set<K> keySet() {
      if (hasUncommittedChanges()) {
           return new HashSet<K>(keySetUncommitted());
      } else {
         HotRodAtomicHashMap<K, V> map = getDeltaMapForRead().copy();
         Set<K> result = new HashSet<K>(keySetUncommitted());
         if (map != null) {
            result.addAll(map.keySet());
         }
         return result;
      }
   }

   @SuppressWarnings("unchecked")
   private Set<K> keySetUncommitted() {
      DeltaAwareCacheEntry entry = lookupEntry();
      return entry != null ?
            (Set<K>) entry.getUncommittedChages().keySet() :
            InfinispanCollections.<K>emptySet();
   }

   @Override
   public Collection<V> values() {
      HotRodAtomicHashMap<K, V> map = getDeltaMapForRead().copy();
      Set<V> result = new HashSet<V>(valuesUncommitted());
      if (map != null) {
         result.addAll(map.values());
      }
      return result;
   }

   @SuppressWarnings("unchecked")
   private Collection<V> valuesUncommitted() {
      DeltaAwareCacheEntry entry = lookupEntry();
      return entry != null ?
            (Collection<V>) entry.getUncommittedChages().values() :
            InfinispanCollections.<V>emptySet();
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      Set<Entry<K, V>> result;
      if (hasUncommittedChanges()) {
         return new HashSet<Entry<K, V>>(entrySetUncommitted());
      } else {
         HotRodAtomicHashMap<K, V> map = getDeltaMapForRead().copy();
         result = new HashSet<Entry<K, V>>();
         if (map != null) {
            result.addAll(map.entrySet());
         }
         return result;
      }
   }

   @SuppressWarnings("unchecked")
   private Set<Entry<K, V>> entrySetUncommitted() {
      DeltaAwareCacheEntry entry = lookupEntry();
      return (Set<Entry<K, V>>)
            (entry != null ? entry.getUncommittedChages().entrySet()
                   : InfinispanCollections.<V>emptySet());
   }

   @Override
   public int size() {
      final HotRodAtomicHashMap<K, V> map = getDeltaMapForRead();
      final int result = sizeUncommitted();
      if (result <= 0 && map != null) {
         return map.size();
      }
      return result;
   }

   public int sizeUncommitted() {
      DeltaAwareCacheEntry entry = lookupEntry();
      return entry != null ? entry.getUncommittedChages().size() : 0;
   }

   public boolean hasUncommittedChanges() {
      DeltaAwareCacheEntry entry = lookupEntry();
      return entry != null && ! entry.getUncommittedChages().isEmpty();
   }

   @Override
   public boolean isEmpty() {
      HotRodAtomicHashMap<K, V> map = getDeltaMapForRead();
      return ! hasUncommittedChanges() && (map == null || map.isEmpty());
   }

   @Override
   public boolean containsKey(Object key) {
      if (hasUncommittedChanges()) {
         return containsKeyUncommitted(key);
      } else {
         HotRodAtomicHashMap<K, V> map = getDeltaMapForRead();
         return map != null ? map.containsKey(key) : false;
      }
   }

   private boolean containsKeyUncommitted(Object key) {
      DeltaAwareCacheEntry entry = lookupEntry();
      return entry != null && entry.getUncommittedChages().containsKey(key);
   }

   @Override
   public boolean containsValue(Object value) {
      if (hasUncommittedChanges()) {
         return containsValueUncommitted(value);
      } else {
         HotRodAtomicHashMap<K, V> map = getDeltaMapForRead();
         return map != null ? map.containsValue(value) : false;
      }
   }

   private boolean containsValueUncommitted(Object value) {
      DeltaAwareCacheEntry entry = lookupEntry();
      return entry != null && entry.getUncommittedChages().containsValue(value);
   }

   @Override
   public V get(Object key) {
      V result = getUncommitted(key);
      if (result == null) {
         HotRodAtomicHashMap<K, V> map = getDeltaMapForRead();
         result = map == null ? null : map.get(key);
      }
      return result;
   }

   @SuppressWarnings("unchecked")
   public V getUncommitted(Object key) {
      DeltaAwareCacheEntry entry = lookupEntry();
      return entry != null ? (V)entry.getUncommittedChages().get(key): null;
   }

   // writers
   @Override
   public V put(K key, V value) {
      HotRodAtomicHashMap<K, V> deltaMapForWrite = null;
      try {
         startAtomic();
         deltaMapForWrite = getDeltaMapForWrite();
         V toReturn = deltaMapForWrite.put(key, value);
         invokeApplyDelta(deltaMapForWrite.getDelta());
         return toReturn;
      } finally {
         endAtomic();
      }
   }

   @Override
   public V remove(Object key) {
      HotRodAtomicHashMap<K, V> deltaMapForWrite = null;
      try {
         startAtomic();
         deltaMapForWrite = getDeltaMapForWrite();
         V toReturn = deltaMapForWrite.remove(key);
         invokeApplyDelta(deltaMapForWrite.getDelta());
         return toReturn;
      } finally {
         endAtomic();
      }
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      HotRodAtomicHashMap<K, V> deltaMapForWrite = null;
      try {
         startAtomic();
         deltaMapForWrite = getDeltaMapForWrite();
         deltaMapForWrite.putAll(m);
         invokeApplyDelta(deltaMapForWrite.getDelta());
      } finally {
         endAtomic();
      }
   }

   @Override
   public void clear() {
      HotRodAtomicHashMap<K, V> deltaMapForWrite = null;
      try {
         startAtomic();
         deltaMapForWrite = getDeltaMapForWrite();
         deltaMapForWrite.clear();
         invokeApplyDelta(deltaMapForWrite.getDelta());
      } finally {
         endAtomic();
      }
   }

   private DeltaAwareCacheEntry lookupEntry() {
//      CacheEntry entry = lookupEntryFromCurrentTransaction();
//      if (entry instanceof DeltaAwareCacheEntry) {
//         return (DeltaAwareCacheEntry)entry;
//      } else {
         return null;
//      }
   }

   private void invokeApplyDelta(AtomicHashMapDelta delta) {
      Collection<?> keys = InfinispanCollections.emptyList();
      if (delta.hasClearOperation()) {
         // if it has clear op we need to lock all keys
         HotRodAtomicHashMap<?, ?> map = (HotRodAtomicHashMap<?, ?>) cache.get(deltaMapKey);
         if (map != null) {
            keys = new ArrayList(map.keySet());
         }
      } else {
         keys = delta.getKeys();
      }
      cache.applyDelta(deltaMapKey, delta, keys);
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder("FineGrainedAtomicHashMapProxy{deltaMapKey=");
      sb.append(deltaMapKey);
      sb.append("}");
      return sb.toString();
   }
}
