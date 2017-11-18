package de.invesdwin.context.persistence.leveldb.phashmap;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;

import com.jamierf.persistenthashmap.PersistentHashMap;
import com.jamierf.persistenthashmap.serializers.ObjectSerializer;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.leveldb.ezdb.ADelegateRangeTable;
import de.invesdwin.context.persistence.leveldb.serde.ExtendedTypeDelegateSerde;
import de.invesdwin.context.persistence.leveldb.timeseries.SerializingCollection;
import de.invesdwin.util.lang.Reflections;
import de.invesdwin.util.math.Bytes;
import ezdb.serde.Serde;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

/**
 * If you need to store large data on disk, it is better to use LevelDB only for an ordered index and store the actual
 * db in a file based persistent hash map. This is because LevelDB has very bad insertion speed when handling large
 * elements.
 */
@ThreadSafe
public abstract class ADelegatePersistentHashMap<K extends Serializable, V extends Serializable>
        implements ConcurrentMap<K, V>, Closeable {

    public static final int DEFAULT_COMPRESSION_LEVEL = SerializingCollection.DEFAULT_COMPRESSION_LEVEL;
    public static final int LARGE_BLOCK_SIZE = SerializingCollection.LARGE_BLOCK_SIZE;
    public static final int DEFAULT_BLOCK_SIZE = SerializingCollection.DEFAULT_BLOCK_SIZE;
    public static final int DEFAULT_SEED = SerializingCollection.DEFAULT_SEED;

    private final String name;
    /**
     * Not synchronized for performance
     */
    private Class<K> keyType;
    /**
     * Not synchronized for performance
     */
    private Class<V> valueType;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    @GuardedBy("this")
    private PersistentHashMap<K, V> delegate;

    public ADelegatePersistentHashMap(final String name) {
        this.name = name;
        this.keySerde = newKeySerde();
        this.valueSerde = newValueSerde();
    }

    public String getName() {
        return name;
    }

    protected synchronized PersistentHashMap<K, V> getDelegate() {
        if (delegate == null) {
            this.delegate = newDelegate();
        }
        return delegate;
    }

    protected PersistentHashMap<K, V> newDelegate() {
        return new PersistentHashMap<>(getFile(), newSerializer(), isForce());
    }

    private ObjectSerializer newSerializer() {
        return new ObjectSerializer() {

            @SuppressWarnings("unchecked")
            @Override
            public void writeObject(final File f, final Serializable o, final boolean force) throws IOException {
                final FileOutputStream fos = new FileOutputStream(f);
                fos.getChannel().force(force);
                final boolean key;
                final byte[] entry;
                if (getKeyType().isAssignableFrom(o.getClass())) {
                    key = true;
                    entry = keySerde.toBytes((K) o);
                } else {
                    key = false;
                    entry = valueSerde.toBytes((V) o);
                }
                final OutputStream compressor = newCompressor(fos);
                try {
                    /*
                     * directly write the key value without wasting time creating another byte array in the serde
                     */
                    IOUtils.write(new byte[] { Bytes.checkedCast(key) }, compressor);
                    IOUtils.write(entry, compressor);
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    IOUtils.closeQuietly(compressor);
                }
            }

            @Override
            public Object readObject(final File f) throws IOException {
                final BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f));
                final InputStream decompressor = newDecompressor(bis);
                try {
                    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    try {
                        IOUtils.copy(decompressor, bos);
                    } catch (final IOException e) {
                        //ignore, end reached
                    }
                    final byte[] bytes = bos.toByteArray();
                    final PersistentEntry entry = PersistentEntrySerde.GET.fromBytes(bytes);
                    if (entry.isKey()) {
                        return keySerde.fromBytes(entry.getEntry());
                    } else {
                        return valueSerde.fromBytes(entry.getEntry());
                    }
                } finally {
                    IOUtils.closeQuietly(decompressor);
                }
            }
        };
    }

    protected boolean isForce() {
        return false;
    }

    protected InputStream newDecompressor(final InputStream in) {
        return SerializingCollection.newDefaultLZ4BlockInputStream(in);
    }

    protected OutputStream newCompressor(final OutputStream out) {
        return newDefaultLZ4BlockOutputStream(out);
    }

    public static LZ4BlockOutputStream newDefaultLZ4BlockOutputStream(final OutputStream out) {
        return newFastLZ4BlockOutputStream(out, DEFAULT_BLOCK_SIZE);
    }

    public static LZ4BlockOutputStream newLargeLZ4BlockOutputStream(final OutputStream out) {
        return newFastLZ4BlockOutputStream(out, LARGE_BLOCK_SIZE);
    }

    public static LZ4BlockOutputStream newFastLZ4BlockOutputStream(final OutputStream out, final int blockSize) {
        return new LZ4BlockOutputStream(out, blockSize, LZ4Factory.fastestInstance().fastCompressor(),
                XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum(), true);
    }

    protected Serde<K> newKeySerde() {
        return new ExtendedTypeDelegateSerde<K>(getKeyType());
    }

    protected Serde<V> newValueSerde() {
        return new ExtendedTypeDelegateSerde<V>(getValueType());
    }

    protected File getFile() {
        return new File(getDirectory(), name);
    }

    protected File getDirectory() {
        return new File(getBaseDirectory(), ADelegateRangeTable.class.getSimpleName());
    }

    protected File getBaseDirectory() {
        return ContextProperties.getHomeDirectory();
    }

    protected Class<K> getKeyType() {
        if (keyType == null) {
            keyType = determineKeyType();
        }
        return keyType;
    }

    @SuppressWarnings("unchecked")
    private Class<K> determineKeyType() {
        return (Class<K>) Reflections.resolveTypeArguments(getClass(), ADelegatePersistentHashMap.class)[0];
    }

    protected Class<V> getValueType() {
        if (valueType == null) {
            valueType = determineValueType();
        }
        return valueType;
    }

    @SuppressWarnings("unchecked")
    private Class<V> determineValueType() {
        return (Class<V>) Reflections.resolveTypeArguments(getClass(), ADelegatePersistentHashMap.class)[1];
    }

    @Override
    public int size() {
        return getDelegate().size();
    }

    @Override
    public boolean isEmpty() {
        return getDelegate().isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return getDelegate().containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return getDelegate().containsValue(value);
    }

    @Override
    public V get(final Object key) {
        return getDelegate().get(key);
    }

    @Override
    public V put(final K key, final V value) {
        return getDelegate().put(key, value);
    }

    @Override
    public V remove(final Object key) {
        return getDelegate().remove(key);
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        getDelegate().putAll(m);
    }

    @Override
    public void clear() {
        getDelegate().clear();
    }

    @Override
    public Set<K> keySet() {
        return getDelegate().keySet();
    }

    @Override
    public Collection<V> values() {
        return getDelegate().values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return getDelegate().entrySet();
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        return getDelegate().putIfAbsent(key, value);
    }

    @Override
    public boolean remove(final Object key, final Object value) {
        return getDelegate().remove(key, value);
    }

    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
        return getDelegate().replace(key, oldValue, newValue);
    }

    @Override
    public V replace(final K key, final V value) {
        return getDelegate().replace(key, value);
    }

    @Override
    public synchronized void close() {
        if (delegate != null) {
            delegate = null;
        }
    }

    public synchronized void deleteTable() {
        close();
        FileUtils.deleteQuietly(getFile());
    }

}
