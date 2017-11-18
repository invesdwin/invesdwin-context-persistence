package de.invesdwin.context.persistence.leveldb.phashmap;

import javax.annotation.concurrent.Immutable;

@Immutable
public class PersistentEntry {

    private final boolean key;
    private final byte[] entry;

    public PersistentEntry(final boolean key, final byte[] entry) {
        this.key = key;
        this.entry = entry;
    }

    public boolean isKey() {
        return key;
    }

    public boolean isValue() {
        return !isKey();
    }

    public byte[] getEntry() {
        return entry;
    }

}
