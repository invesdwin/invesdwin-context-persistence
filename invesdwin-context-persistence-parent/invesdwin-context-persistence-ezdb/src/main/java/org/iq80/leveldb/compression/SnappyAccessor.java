package org.iq80.leveldb.compression;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class SnappyAccessor {

    public static final Compression SNAPPY = Snappy.SNAPPY;

    private SnappyAccessor() {}

}
