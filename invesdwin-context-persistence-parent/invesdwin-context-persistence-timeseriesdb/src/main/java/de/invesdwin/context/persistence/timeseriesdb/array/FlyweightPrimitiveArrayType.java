package de.invesdwin.context.persistence.timeseriesdb.array;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum FlyweightPrimitiveArrayType {
    Byte,
    Boolean,
    Double,
    Long,
    Int;
}
