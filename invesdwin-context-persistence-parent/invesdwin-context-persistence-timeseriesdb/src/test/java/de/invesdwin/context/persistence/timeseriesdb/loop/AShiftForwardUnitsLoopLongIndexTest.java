package de.invesdwin.context.persistence.timeseriesdb.loop;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.time.date.BisectDuplicateKeyHandling;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;

// CHECKSTYLE:OFF
@NotThreadSafe
public class AShiftForwardUnitsLoopLongIndexTest extends ATest {
    //CHECKSTYLE:ON

    private static final List<FDate> DATES;

    static {
        DATES = new ArrayList<>();
        for (int i = 2000; i <= 2010; i++) {
            DATES.add(FDateBuilder.newDate(i));
        }
    }

    @Test
    public void testShiftForward() {
        for (int shiftForwardUnits = 0; shiftForwardUnits < DATES.size(); shiftForwardUnits++) {
            for (int i = 0; i < DATES.size(); i++) {
                final FDate request = DATES.get(i);
                log.warn("*** " + i + "+" + shiftForwardUnits + ": " + request + " ***");
                final AShiftForwardUnitsLoopLongIndex<FDate> loop = newLoop(request, shiftForwardUnits);
                loop.loop();
                final FDate nextValue = loop.getNextValue();
                log.info("nextValue " + nextValue);

                final int expectedIndex = i + shiftForwardUnits;
                if (expectedIndex >= DATES.size()) {
                    Assertions.checkEquals(DATES.get(DATES.size() - 1), nextValue);
                } else {
                    Assertions.checkEquals(DATES.get(expectedIndex), nextValue);
                }

                /////////////// minus
                final FDate requestMinus = request.addMilliseconds(-1);
                final AShiftForwardUnitsLoopLongIndex<FDate> loopMinus = newLoop(requestMinus, shiftForwardUnits);
                loopMinus.loop();
                final FDate nextValueMinus = loopMinus.getNextValue();
                log.info("nextValueMinus " + nextValueMinus);

                final int expectedIndexMinus;
                if (shiftForwardUnits == 0 || i == 0) {
                    expectedIndexMinus = expectedIndex;
                } else {
                    expectedIndexMinus = i + shiftForwardUnits;
                }
                if (expectedIndexMinus >= DATES.size()) {
                    Assertions.checkEquals(DATES.get(DATES.size() - 1), nextValueMinus);
                } else {
                    Assertions.checkEquals(DATES.get(expectedIndexMinus), nextValueMinus);
                }

                ///////// plus
                final FDate requestPlus = request.addMilliseconds(1);
                final AShiftForwardUnitsLoopLongIndex<FDate> loopPlus = newLoop(requestPlus, shiftForwardUnits);
                loopPlus.loop();
                final FDate nextValuePlus = loopPlus.getNextValue();
                log.info("nextValuePlus " + nextValuePlus);

                final int expectedIndexPlus = i + Integers.max(shiftForwardUnits + 1, 1);
                if (expectedIndexPlus >= DATES.size()) {
                    if (shiftForwardUnits == 0 || i >= DATES.size() - 1) {
                        Assertions.checkNull(nextValuePlus);
                    } else {
                        Assertions.checkEquals(DATES.get(DATES.size() - 1), nextValuePlus);
                    }
                } else {
                    Assertions.checkEquals(DATES.get(expectedIndexPlus), nextValuePlus);
                }
            }
        }
    }

    private AShiftForwardUnitsLoopLongIndex<FDate> newLoop(final FDate request, final int shiftForwardUnits) {
        return new AShiftForwardUnitsLoopLongIndex<FDate>(request, shiftForwardUnits) {

            @Override
            protected FDate getLatestValue(final long index) {
                if (index >= DATES.size()) {
                    return DATES.get(DATES.size() - 1);
                }
                return DATES.get(Integers.checkedCast(index));
            }

            @Override
            protected long getLatestValueIndex(final FDate date) {
                return FDates.bisect(DATES.toArray(FDate.EMPTY_ARRAY), request, BisectDuplicateKeyHandling.UNDEFINED);
            }

            @Override
            protected FDate extractEndTime(final FDate value) {
                return value;
            }

            @Override
            protected long size() {
                return DATES.size();
            }
        };
    }

}
