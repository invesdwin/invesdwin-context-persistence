package de.invesdwin.context.persistence.chronicle;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.streams.buffer.delegate.ChronicleDelegateByteBuffer;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;

@Immutable
public class ChronicleMarshaller<T> implements BytesReader<T>, BytesWriter<T> {

    private static final int SIZE_INDEX = 0;
    private static final int SIZE_SIZE = Integer.BYTES;

    private static final int VALUE_INDEX = SIZE_INDEX + SIZE_SIZE;

    private final ISerde<T> serde;

    public ChronicleMarshaller(final ISerde<T> serde) {
        this.serde = serde;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void write(final net.openhft.chronicle.bytes.Bytes out, final T value) {
        final ChronicleDelegateByteBuffer buffer = new ChronicleDelegateByteBuffer(out);
        final int positionBefore = (int) out.writePosition();
        final int length = serde.toBuffer(buffer.newSliceFrom(positionBefore + VALUE_INDEX), value);
        buffer.putInt(positionBefore + SIZE_INDEX, length);
        out.writePosition(positionBefore + VALUE_INDEX + length);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public T read(final net.openhft.chronicle.bytes.Bytes in, final T using) {
        final ChronicleDelegateByteBuffer buffer = new ChronicleDelegateByteBuffer(in);
        final int positionBefore = (int) in.readPosition();
        final int length = buffer.getInt(positionBefore + SIZE_INDEX);
        in.readPosition(positionBefore + VALUE_INDEX + length);
        if (length == 0) {
            return null;
        }
        final T value = serde.fromBuffer(buffer.newSlice(positionBefore + VALUE_INDEX, length), length);
        return value;
    }

}
