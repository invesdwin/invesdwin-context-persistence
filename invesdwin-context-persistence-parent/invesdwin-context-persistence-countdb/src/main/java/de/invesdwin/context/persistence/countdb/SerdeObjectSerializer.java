package de.invesdwin.context.persistence.countdb;

import javax.annotation.concurrent.Immutable;

import be.bagofwords.db.methods.DataStream;
import be.bagofwords.db.methods.ObjectSerializer;
import de.invesdwin.util.marshallers.serde.ISerde;

@Immutable
public class SerdeObjectSerializer<V> implements ObjectSerializer<V> {

    private final ISerde<V> serde;

    public SerdeObjectSerializer(final ISerde<V> serde) {
        this.serde = serde;
    }

    @Override
    public void writeValue(final V obj, final DataStream ds) {
        ds.writeBytes(serde.toBytes(obj));
    }

    @Override
    public V readValue(final DataStream ds, final int size) {
        return serde.fromBytes(ds.readBytes(size));
    }

    @Override
    public int getObjectSize() {
        return -1;
    }

}
