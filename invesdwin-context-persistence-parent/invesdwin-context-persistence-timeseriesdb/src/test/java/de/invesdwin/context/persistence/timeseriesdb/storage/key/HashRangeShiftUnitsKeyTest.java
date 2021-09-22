package de.invesdwin.context.persistence.timeseriesdb.storage.key;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Test;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FDateBuilder;

@NotThreadSafe
public class HashRangeShiftUnitsKeyTest extends ATest {

    @Test
    public void testCompare() {
        final List<HashRangeShiftUnitsKey> sortedKeys = new ArrayList<>();
        sortedKeys.add(new HashRangeShiftUnitsKey("1", FDateBuilder.newDate(1900), Integer.MAX_VALUE)); //0
        sortedKeys.add(new HashRangeShiftUnitsKey("1", FDateBuilder.newDate(2000), 1)); //1
        sortedKeys.add(new HashRangeShiftUnitsKey("1", FDateBuilder.newDate(2000), 2)); //2
        sortedKeys.add(new HashRangeShiftUnitsKey("1", FDateBuilder.newDate(2001), 2)); //3

        final List<HashRangeShiftUnitsKey> randomKeys = new ArrayList<>();
        randomKeys.add(sortedKeys.get(2));
        randomKeys.add(sortedKeys.get(3));
        randomKeys.add(sortedKeys.get(1));
        randomKeys.add(sortedKeys.get(0));

        Assertions.assertThat(sortedKeys).isNotEqualTo(randomKeys);

        Collections.sort(randomKeys);

        Assertions.assertThat(sortedKeys).isEqualTo(randomKeys);

    }

}
