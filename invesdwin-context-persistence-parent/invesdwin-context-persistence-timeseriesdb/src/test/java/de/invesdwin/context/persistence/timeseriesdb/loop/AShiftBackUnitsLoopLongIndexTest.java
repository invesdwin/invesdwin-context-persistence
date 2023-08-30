package de.invesdwin.context.persistence.timeseriesdb.loop;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.time.date.BisectDuplicateKeyHandling;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FDateBuilder;
import de.invesdwin.util.time.date.FDates;

// CHECKSTYLE:OFF
@NotThreadSafe
public class AShiftBackUnitsLoopLongIndexTest extends ATest {
    //CHECKSTYLE:ON

    private static final List<FDate> DATES_REVERSE;
    private static final List<FDate> DATES;

    static {
        DATES_REVERSE = new ArrayList<>();
        for (int i = 2000; i <= 2010; i++) {
            DATES_REVERSE.add(FDateBuilder.newDate(i));
        }
        DATES = new ArrayList<>(DATES_REVERSE);
        Collections.reverse(DATES);
    }

    @Test
    public void testShiftBackward() {
        for (int shiftBackUnits = 0; shiftBackUnits < DATES.size(); shiftBackUnits++) {
            for (int i = 0; i < DATES.size(); i++) {
                final FDate request = DATES.get(i);
                log.warn("*** " + i + "+" + shiftBackUnits + ": " + request + " ***");
                final AShiftBackUnitsLoopLongIndex<FDate> loop = newLoop(request, shiftBackUnits);
                loop.loop();
                final FDate prevValue = loop.getPrevValue();
                log.info("prevValue " + prevValue);

                final int expectedIndex = i + shiftBackUnits;
                if (expectedIndex >= DATES.size()) {
                    Assertions.checkEquals(DATES.get(DATES.size() - 1), prevValue);
                } else {
                    Assertions.checkEquals(DATES.get(expectedIndex), prevValue);
                }

                /////////////// minus
                final FDate requestMinus = request.addMilliseconds(-1);
                final AShiftBackUnitsLoopLongIndex<FDate> loopMinus = newLoop(requestMinus, shiftBackUnits);
                loopMinus.loop();
                final FDate prevValueMinus = loopMinus.getPrevValue();
                log.info("prevValueMinus " + prevValueMinus);

                final int expectedIndexMinus;
                if (shiftBackUnits == 0 || i == 0) {
                    expectedIndexMinus = expectedIndex + 1;
                } else {
                    expectedIndexMinus = i + shiftBackUnits + 1;
                }
                if (expectedIndexMinus >= DATES.size()) {
                    if (shiftBackUnits == 0 || i >= DATES.size() - 1) {
                        Assertions.checkNull(prevValueMinus);
                    } else {
                        Assertions.checkEquals(DATES.get(DATES.size() - 1), prevValueMinus);
                    }
                } else {
                    Assertions.checkEquals(DATES.get(expectedIndexMinus), prevValueMinus);
                }

                ///////// plus
                final FDate requestPlus = request.addMilliseconds(1);
                final AShiftBackUnitsLoopLongIndex<FDate> loopPlus = newLoop(requestPlus, shiftBackUnits);
                loopPlus.loop();
                final FDate prevValuePlus = loopPlus.getPrevValue();
                log.info("prevValuePlus " + prevValuePlus);

                final int expectedIndexPlus = i + shiftBackUnits;
                if (expectedIndexPlus >= DATES.size()) {
                    Assertions.checkEquals(DATES.get(DATES.size() - 1), prevValuePlus);
                } else {
                    Assertions.checkEquals(DATES.get(expectedIndexPlus), prevValuePlus);
                }
            }
        }
    }

    private AShiftBackUnitsLoopLongIndex<FDate> newLoop(final FDate request, final int shiftBackUnits) {
        return new AShiftBackUnitsLoopLongIndex<FDate>(request, shiftBackUnits) {

            @Override
            protected FDate getLatestValue(final long index) {
                if (index >= DATES_REVERSE.size()) {
                    return DATES_REVERSE.get(DATES_REVERSE.size() - 1);
                }
                return DATES_REVERSE.get(Integers.checkedCast(index));
            }

            @Override
            protected long getLatestValueIndex(final FDate date) {
                return FDates.bisect(DATES_REVERSE.toArray(FDate.EMPTY_ARRAY), request,
                        BisectDuplicateKeyHandling.UNDEFINED);
            }

            @Override
            protected FDate extractEndTime(final FDate value) {
                return value;
            }

        };
    }

}
