package de.invesdwin.context.persistence.timeseriesdb;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.persistentmap.IPersistentMapFactory;
import de.invesdwin.context.integration.persistentmap.large.LargePersistentMapFactory;
import de.invesdwin.context.persistence.chronicle.PersistentChronicleMapFactory;
import de.invesdwin.context.persistence.ezdb.PersistentEzdbMapFactory;
import de.invesdwin.context.persistence.mapdb.PersistentMapDBFactory;

@Immutable
public enum PersistentMapType {
    FAST {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            //otherwise use chronicle map for faster put/get
            return new PersistentChronicleMapFactory<>();
        }
    },
    SAFE {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            //leveldb does not have any hiccups regarding force closed applications
            return new PersistentEzdbMapFactory<>();
        }
    },
    MEDIUM {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            //mapdb has a smaller footprint on disk, thus prefer that for larger entries
            return new PersistentMapDBFactory<K, V>();
        }
    },
    LARGE_SAFE {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            return new LargePersistentMapFactory<>(SAFE.newFactory());
        }
    },
    LARGE_FAST {
        @Override
        public <K, V> IPersistentMapFactory<K, V> newFactory() {
            return new LargePersistentMapFactory<>(FAST.newFactory());
        }
    };

    public abstract <K, V> IPersistentMapFactory<K, V> newFactory();

}
