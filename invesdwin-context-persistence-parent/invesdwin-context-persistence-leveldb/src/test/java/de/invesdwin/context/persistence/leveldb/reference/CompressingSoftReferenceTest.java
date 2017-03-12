package de.invesdwin.context.persistence.leveldb.reference;

import java.util.LinkedHashSet;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import org.junit.Ignore;
import org.junit.Test;

import de.invesdwin.context.persistence.leveldb.serde.FastSerializingSerde;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.math.decimal.Decimal;

@ThreadSafe
public class CompressingSoftReferenceTest extends ATest {

    @Test
    public void testSerialization() {
        final CompressingSoftReference<Decimal> ref = new CompressingSoftReference<Decimal>(new Decimal("100"),
                new FastSerializingSerde<Decimal>(false, Decimal.class));
        Assertions.assertThat(ref.get()).isNotNull();
        ref.clear();
        Assertions.assertThat(ref.get()).isNotNull();
        ref.close();
        Assertions.assertThat(ref.get()).isNull();
    }

    @Test
    @Ignore("manual test")
    public void testOutOfMemory() {
        final Set<CompressingSoftReference<Decimal>> refs = new LinkedHashSet<CompressingSoftReference<Decimal>>();
        Decimal curValue = Decimal.ZERO;
        while (true) {
            curValue = curValue.add(Decimal.ONE);
            final CompressingSoftReference<Decimal> ref = new CompressingSoftReference<Decimal>(curValue,
                    new FastSerializingSerde<Decimal>(false, Decimal.class));
            refs.add(ref);
            Assertions.assertThat(ref.get()).isNotNull();
        }
    }

}
