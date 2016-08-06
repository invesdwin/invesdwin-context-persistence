// CHECKSTYLE:OFF
package de.invesdwin.context.persistence.leveldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.NoSuchElementException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.persistence.leveldb.ADelegateRangeTable;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDateBuilder;
import ezdb.RawTableRow;
import ezdb.Table;
import ezdb.TableIterator;

@NotThreadSafe
public class TestEzLevelDbJni extends ATest {
    private static final String HASHKEY_ONE = "1";
    private static final FDate MAX_DATE = FDate.MAX_DATE;
    private static final FDate MIN_DATE = FDate.MIN_DATE;
    final FDate now = FDateBuilder.newDate(2000);
    final FDate oneFDate = now.addDays(1);
    final FDate twoFDate = now.addDays(2);
    final FDate threeFDate = now.addDays(3);

    private final FDate oneFDatePlus = new FDate(oneFDate.millisValue() + 1);
    private final FDate twoFDatePlus = new FDate(twoFDate.millisValue() + 1);
    private final FDate threeFDatePlus = new FDate(threeFDate.millisValue() + 1);

    private final FDate oneFDateMinus = new FDate(oneFDate.millisValue() - 1);
    private final FDate twoFDateMinus = new FDate(twoFDate.millisValue() - 1);
    private final FDate threeFDateMinus = new FDate(threeFDate.millisValue() - 1);

    private ADelegateRangeTable<String, FDate, Integer> reverseRangeTable;

    protected static final File ROOT = ContextProperties.getHomeDirectory();
    protected ADelegateRangeTable<Integer, Integer, Integer> table;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        table = new ADelegateRangeTable<Integer, Integer, Integer>("test") {
            @Override
            protected boolean allowHasNext() {
                return true;
            }

            @Override
            protected boolean allowPutWithoutBatch() {
                return true;
            }

        };

        reverseRangeTable = new ADelegateRangeTable<String, FDate, Integer>("testInverseOrder") {
            @Override
            protected boolean allowHasNext() {
                return true;
            }

            @Override
            protected boolean allowPutWithoutBatch() {
                return true;
            }

        };
        reverseRangeTable.put("0", oneFDate, -1);
        reverseRangeTable.put("0", twoFDate, -2);
        reverseRangeTable.put("0", threeFDate, -3);
        reverseRangeTable.put(HASHKEY_ONE, oneFDate, 1);
        reverseRangeTable.put(HASHKEY_ONE, twoFDate, 2);
        reverseRangeTable.put(HASHKEY_ONE, threeFDate, 3);
        reverseRangeTable.put("2", oneFDate, -10);
        reverseRangeTable.put("2", twoFDate, -20);
        reverseRangeTable.put("2", threeFDate, -30);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        reverseRangeTable.close();
        reverseRangeTable.deleteTable();

        table.close();
        table.deleteTable();
        FileUtils.deleteQuietly(ROOT);
    }

    @Test
    public void range21Reverse() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, twoFDate,
                oneFDate);
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range21ReversePlus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, twoFDatePlus,
                oneFDatePlus);
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range21ReverseMinus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, twoFDateMinus,
                oneFDateMinus);
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range32Reverse() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, threeFDate,
                twoFDate);
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range32ReversePlus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, threeFDatePlus,
                twoFDatePlus);
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range32ReverseMinus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, threeFDateMinus,
                twoFDateMinus);
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range12Reverse() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, oneFDate,
                twoFDate);
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range23Reverse() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, twoFDate,
                threeFDate);
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range21() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, twoFDate, oneFDate);
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range32() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, threeFDate, twoFDate);
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range12() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, oneFDate, twoFDate);
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range12Plus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, oneFDatePlus,
                twoFDatePlus);
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range12Minus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, oneFDateMinus,
                twoFDateMinus);
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range23() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, twoFDate, threeFDate);
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range23Plus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, twoFDatePlus,
                threeFDatePlus);
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range23Minus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, twoFDateMinus,
                threeFDateMinus);
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range2Reverse() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, twoFDate);
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range2ReversePlus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, twoFDatePlus);
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range2ReverseMinus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, twoFDateMinus);
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeNoneReverse() {
        final TableIterator<String, FDate, Integer> rangeNoneReverse = reverseRangeTable.rangeReverse(HASHKEY_ONE);
        Assert.assertEquals(3, (int) rangeNoneReverse.next().getValue());
        Assert.assertEquals(2, (int) rangeNoneReverse.next().getValue());
        Assert.assertEquals(1, (int) rangeNoneReverse.next().getValue());
        Assert.assertFalse(rangeNoneReverse.hasNext());
        try {
            rangeNoneReverse.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeNullReverse() {
        final TableIterator<String, FDate, Integer> rangeNoneReverse = reverseRangeTable.rangeReverse(HASHKEY_ONE,
                null);
        Assert.assertEquals(3, (int) rangeNoneReverse.next().getValue());
        Assert.assertEquals(2, (int) rangeNoneReverse.next().getValue());
        Assert.assertEquals(1, (int) rangeNoneReverse.next().getValue());
        Assert.assertFalse(rangeNoneReverse.hasNext());
        try {
            rangeNoneReverse.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeNullNullReverse() {
        final TableIterator<String, FDate, Integer> rangeNoneReverse = reverseRangeTable.rangeReverse(HASHKEY_ONE, null,
                null);
        Assert.assertEquals(3, (int) rangeNoneReverse.next().getValue());
        Assert.assertEquals(2, (int) rangeNoneReverse.next().getValue());
        Assert.assertEquals(1, (int) rangeNoneReverse.next().getValue());
        Assert.assertFalse(rangeNoneReverse.hasNext());
        try {
            rangeNoneReverse.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range2NullReverse() {
        final TableIterator<String, FDate, Integer> rangeNoneReverse = reverseRangeTable.rangeReverse(HASHKEY_ONE,
                twoFDate, null);
        Assert.assertEquals(2, (int) rangeNoneReverse.next().getValue());
        Assert.assertEquals(1, (int) rangeNoneReverse.next().getValue());
        Assert.assertFalse(rangeNoneReverse.hasNext());
        try {
            rangeNoneReverse.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeNull2Reverse() {
        final TableIterator<String, FDate, Integer> rangeNoneReverse = reverseRangeTable.rangeReverse(HASHKEY_ONE, null,
                twoFDate);
        Assert.assertEquals(3, (int) rangeNoneReverse.next().getValue());
        Assert.assertEquals(2, (int) rangeNoneReverse.next().getValue());
        Assert.assertFalse(rangeNoneReverse.hasNext());
        try {
            rangeNoneReverse.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeMaxNullReverse() {
        final TableIterator<String, FDate, Integer> rangeNoneReverse = reverseRangeTable.rangeReverse(HASHKEY_ONE,
                MAX_DATE, null);
        Assert.assertEquals(3, (int) rangeNoneReverse.next().getValue());
        Assert.assertEquals(2, (int) rangeNoneReverse.next().getValue());
        Assert.assertEquals(1, (int) rangeNoneReverse.next().getValue());
        Assert.assertFalse(rangeNoneReverse.hasNext());
        try {
            rangeNoneReverse.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeNullMaxReverse() {
        final TableIterator<String, FDate, Integer> rangeNoneReverse = reverseRangeTable.rangeReverse(HASHKEY_ONE, null,
                MAX_DATE);
        Assert.assertFalse(rangeNoneReverse.hasNext());
        try {
            rangeNoneReverse.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeMinNullReverse() {
        final TableIterator<String, FDate, Integer> rangeNoneReverse = reverseRangeTable.rangeReverse(HASHKEY_ONE,
                MIN_DATE, null);
        Assert.assertFalse(rangeNoneReverse.hasNext());
        try {
            rangeNoneReverse.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeNullMinReverse() {
        final TableIterator<String, FDate, Integer> rangeNoneReverse = reverseRangeTable.rangeReverse(HASHKEY_ONE, null,
                MIN_DATE);
        Assert.assertEquals(3, (int) rangeNoneReverse.next().getValue());
        Assert.assertEquals(2, (int) rangeNoneReverse.next().getValue());
        Assert.assertEquals(1, (int) rangeNoneReverse.next().getValue());
        Assert.assertFalse(rangeNoneReverse.hasNext());
        try {
            rangeNoneReverse.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range3Reverse() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, threeFDate);
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range3ReversePlus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, threeFDatePlus);
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range3ReverseMinus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE,
                threeFDateMinus);
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range2() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, twoFDate);
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range2Plus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, twoFDatePlus);
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range2Minus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, twoFDateMinus);
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeNone() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE);
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeNull() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, null);
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeNullNull() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, null, null);
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range2Null() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, twoFDate, null);
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeNull2() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, null, twoFDate);
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeMaxNull() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, MAX_DATE, null);
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeNullMax() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, null, MAX_DATE);
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeMinNull() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, MIN_DATE, null);
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeNullMin() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, null, MIN_DATE);
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range3() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, threeFDate);
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range3Plus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, threeFDatePlus);
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void range3Minus() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, threeFDateMinus);
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeNow() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, now);
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeNowReverse() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, now);
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeMin() {
        final TableIterator<String, FDate, Integer> rangeMin = reverseRangeTable.range(HASHKEY_ONE, MIN_DATE);
        Assert.assertEquals(1, (int) rangeMin.next().getValue());
        Assert.assertEquals(2, (int) rangeMin.next().getValue());
        Assert.assertEquals(3, (int) rangeMin.next().getValue());
        Assert.assertFalse(rangeMin.hasNext());
        try {
            rangeMin.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeMinMax() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, MIN_DATE, MAX_DATE);
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeMaxMin() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, MAX_DATE, MIN_DATE);
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeMinReverse() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, MIN_DATE);
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeMinMaxReverse() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, MIN_DATE,
                MAX_DATE);
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeMaxMinReverse() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, MAX_DATE,
                MIN_DATE);
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeMaxReverse() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.rangeReverse(HASHKEY_ONE, MAX_DATE);
        Assert.assertEquals(3, (int) range.next().getValue());
        Assert.assertEquals(2, (int) range.next().getValue());
        Assert.assertEquals(1, (int) range.next().getValue());
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void rangeMax() {
        final TableIterator<String, FDate, Integer> range = reverseRangeTable.range(HASHKEY_ONE, MAX_DATE);
        Assert.assertFalse(range.hasNext());
        try {
            range.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void getNone() {
        Assert.assertEquals(null, reverseRangeTable.get(HASHKEY_ONE));
    }

    @Test
    public void getNull() {
        Assert.assertEquals(null, reverseRangeTable.get(HASHKEY_ONE, null));
    }

    @Test
    public void get2() {
        Assert.assertEquals((Integer) 2, reverseRangeTable.get(HASHKEY_ONE, twoFDate));
    }

    @Test
    public void getMin() {
        Assert.assertEquals(null, reverseRangeTable.get(HASHKEY_ONE, MIN_DATE));
    }

    @Test
    public void getMax() {
        Assert.assertEquals(null, reverseRangeTable.get(HASHKEY_ONE, MAX_DATE));
    }

    @Test
    public void get2Plus() {
        Assert.assertEquals(null, reverseRangeTable.get(HASHKEY_ONE, twoFDatePlus));
    }

    @Test
    public void get2Minus() {
        Assert.assertEquals(null, reverseRangeTable.get(HASHKEY_ONE, twoFDateMinus));
    }

    @Test
    public void getLastNone() {
        Assert.assertEquals((Integer) 3, reverseRangeTable.getLatest(HASHKEY_ONE).getValue());
    }

    @Test
    public void getLastNull() {
        Assert.assertEquals((Integer) 3, reverseRangeTable.getLatest(HASHKEY_ONE, null).getValue());
    }

    @Test
    public void getLast2() {
        Assert.assertEquals((Integer) 2, reverseRangeTable.getLatest(HASHKEY_ONE, twoFDate).getValue());
    }

    @Test
    public void getLastMin() {
        Assert.assertEquals((Integer) 1, reverseRangeTable.getLatest(HASHKEY_ONE, MIN_DATE).getValue());
    }

    @Test
    public void getLastMax() {
        Assert.assertEquals((Integer) 3, reverseRangeTable.getLatest(HASHKEY_ONE, MAX_DATE).getValue());
    }

    @Test
    public void getLast2Plus() {
        Assert.assertEquals((Integer) 2, reverseRangeTable.getLatest(HASHKEY_ONE, twoFDatePlus).getValue());
    }

    @Test
    public void getLast2Minus() {
        Assert.assertEquals((Integer) 1, reverseRangeTable.getLatest(HASHKEY_ONE, twoFDateMinus).getValue());
    }

    @Test
    public void testNulls() {
        assertEquals(null, table.get(1));
        assertEquals(null, table.get(1, 1));
        assertTrue(!table.range(1).hasNext());
        assertTrue(!table.range(1, 2).hasNext());
        assertTrue(!table.range(1, 1, 2).hasNext());
    }

    @Test
    public void testPutGetH() {
        final Table<Integer, Integer> table = new ADelegateRangeTable<Integer, Void, Integer>("test-simple") {
            @Override
            protected boolean allowHasNext() {
                return true;
            }

            @Override
            protected boolean allowPutWithoutBatch() {
                return true;
            }
        };
        table.put(1, 1);
        assertEquals(new Integer(1), table.get(1));
        table.put(1, 2);
        assertEquals(new Integer(2), table.get(1));
        table.close();
    }

    @Test
    public void testPutGetHR() {
        table.put(1, 1);
        table.put(1, 1, 3);
        assertEquals(new Integer(1), table.get(1));
        assertEquals(new Integer(3), table.get(1, 1));
        table.put(1, 1, 4);
        assertEquals(new Integer(4), table.get(1, 1));
    }

    @Test
    public void testRangeH() {
        TableIterator<Integer, Integer, Integer> it = table.range(1);
        table.put(1, 2);
        table.put(1, 1, 4);
        table.put(2, 1, 4);
        it.close();
        it = table.range(1);
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, null, 2), it.next());
        assertTrue(it.hasNext());
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
        assertTrue(!it.hasNext());
        it.close();
    }

    @Test
    public void testRangeHR() {
        table.put(1, 2);
        table.put(1, 1, 4);
        TableIterator<Integer, Integer, Integer> it = table.range(1, null);
        assertTrue(it.hasNext());
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, null, 2), it.next());
        assertTrue(it.hasNext());
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
        assertTrue(!it.hasNext());
        it.close();
        it = table.range(1, 1);
        assertTrue(it.hasNext());
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
        assertTrue(!it.hasNext());
        table.put(1, 2, 5);
        table.put(2, 2, 5);
        it.close();
        it = table.range(1, 1);
        assertTrue(it.hasNext());
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
        assertTrue(it.hasNext());
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, 2, 5), it.next());
        assertTrue(!it.hasNext());
        it.close();
        it = table.range(1, null);
        assertTrue(it.hasNext());
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, null, 2), it.next());
        assertTrue(it.hasNext());
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
        assertTrue(it.hasNext());
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, 2, 5), it.next());
        assertTrue(!it.hasNext());
        it.close();
    }

    @Test
    public void testRangeHRR() {
        table.put(1, 2);
        table.put(1, 1, 4);
        TableIterator<Integer, Integer, Integer> it = table.range(1, null, 2);
        assertTrue(it.hasNext());
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, null, 2), it.next());
        assertTrue(it.hasNext());
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
        assertTrue(!it.hasNext());
        it.close();
        it = table.range(1, 1, 2);
        assertTrue(it.hasNext());
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
        assertTrue(!it.hasNext());
        table.put(1, 2, 5);
        table.put(1, 3, 5);
        it.close();
        it = table.range(1, 1, 3);
        assertTrue(it.hasNext());
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, 1, 4), it.next());
        assertTrue(it.hasNext());
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, 2, 5), it.next());
        assertTrue(it.hasNext());
        assertEquals(new RawTableRow<Integer, Integer, Integer>(1, 3, 5), it.next());
        assertTrue(!it.hasNext());
        it.close();
    }

    @Test
    public void testDeleteH() {
        table.put(1, 1);
        assertEquals(new Integer(1), table.get(1));
        table.delete(1);
        assertEquals(null, table.get(1));
    }

    @Test
    public void testDeleteHR() {
        table.put(1, 1);
        table.put(1, 1, 2);
        assertEquals(new Integer(1), table.get(1));
        assertEquals(new Integer(2), table.get(1, 1));
        table.delete(1, 1);
        assertEquals(new Integer(1), table.get(1));
        assertEquals(null, table.get(1, 1));
    }

    @Test
    public void testInverseOrder() {
        final TableIterator<String, FDate, Integer> range3 = reverseRangeTable.range(HASHKEY_ONE, now);
        Assert.assertEquals((Integer) 1, range3.next().getValue());
        Assert.assertEquals((Integer) 2, range3.next().getValue());
        Assert.assertEquals((Integer) 3, range3.next().getValue());
        Assert.assertFalse(range3.hasNext());
        try {
            range3.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
        range3.close(); // should already be closed but should not cause an
        // error when calling again

        final TableIterator<String, FDate, Integer> rangeNone = reverseRangeTable.range(HASHKEY_ONE);
        Assert.assertEquals((Integer) 1, rangeNone.next().getValue());
        Assert.assertEquals((Integer) 2, rangeNone.next().getValue());
        Assert.assertEquals((Integer) 3, rangeNone.next().getValue());
        Assert.assertFalse(rangeNone.hasNext());
        try {
            rangeNone.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }

        final TableIterator<String, FDate, Integer> rangeMin = reverseRangeTable.range(HASHKEY_ONE, MIN_DATE);
        Assert.assertEquals((Integer) 1, rangeMin.next().getValue());
        Assert.assertEquals((Integer) 2, rangeMin.next().getValue());
        Assert.assertEquals((Integer) 3, rangeMin.next().getValue());
        Assert.assertFalse(rangeMin.hasNext());
        try {
            rangeMin.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }

        final TableIterator<String, FDate, Integer> rangeMax = reverseRangeTable.range(HASHKEY_ONE, MAX_DATE);
        Assert.assertFalse(rangeMax.hasNext());
        try {
            rangeMax.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }

        final TableIterator<String, FDate, Integer> range2 = reverseRangeTable.range(HASHKEY_ONE, twoFDate);
        Assert.assertEquals((Integer) 2, range2.next().getValue());
        Assert.assertEquals((Integer) 3, range2.next().getValue());
        Assert.assertFalse(range2.hasNext());
        try {
            range2.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }

        testReverse();

        testGetLatestForRange();
    }

    private void testGetLatestForRange() {
        Assert.assertEquals((Integer) 1, reverseRangeTable.getLatest(HASHKEY_ONE, oneFDate).getValue());
        Assert.assertEquals((Integer) 2, reverseRangeTable.getLatest(HASHKEY_ONE, twoFDate).getValue());
        Assert.assertEquals((Integer) 3, reverseRangeTable.getLatest(HASHKEY_ONE, threeFDate).getValue());

        Assert.assertEquals((Integer) 1, reverseRangeTable.getLatest(HASHKEY_ONE, oneFDateMinus).getValue());
        Assert.assertEquals((Integer) 1, reverseRangeTable.getLatest(HASHKEY_ONE, twoFDateMinus).getValue());
        Assert.assertEquals((Integer) 2, reverseRangeTable.getLatest(HASHKEY_ONE, threeFDateMinus).getValue());
        Assert.assertEquals((Integer) 3,
                reverseRangeTable.getLatest(HASHKEY_ONE, threeFDate.addMilliseconds(1)).getValue());
        Assert.assertEquals((Integer) 3, reverseRangeTable.getLatest(HASHKEY_ONE, threeFDate.addDays(1)).getValue());
        Assertions.assertThat(reverseRangeTable.getLatest(HASHKEY_ONE, threeFDate.addMilliseconds(-1)).getValue())
                .isEqualTo(2);
        Assertions.assertThat(reverseRangeTable.getLatest(HASHKEY_ONE, threeFDate.addMilliseconds(1)).getValue())
                .isEqualTo(3);
        Assert.assertEquals((Integer) 1, reverseRangeTable.getLatest(HASHKEY_ONE, MIN_DATE).getValue());
        Assert.assertEquals((Integer) 3, reverseRangeTable.getLatest(HASHKEY_ONE, MAX_DATE).getValue());
    }

    private void testReverse() {
        final TableIterator<String, FDate, Integer> range3Reverse = reverseRangeTable.rangeReverse(HASHKEY_ONE,
                threeFDate);
        Assert.assertEquals((Integer) 3, range3Reverse.next().getValue());
        Assert.assertEquals((Integer) 2, range3Reverse.next().getValue());
        Assert.assertEquals((Integer) 1, range3Reverse.next().getValue());
        Assert.assertFalse(range3Reverse.hasNext());
        try {
            range3Reverse.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }

        final TableIterator<String, FDate, Integer> rangeNoneReverse = reverseRangeTable.rangeReverse(HASHKEY_ONE);
        Assert.assertEquals((Integer) 3, rangeNoneReverse.next().getValue());
        Assert.assertEquals((Integer) 2, rangeNoneReverse.next().getValue());
        Assert.assertEquals((Integer) 1, rangeNoneReverse.next().getValue());
        Assert.assertFalse(rangeNoneReverse.hasNext());
        try {
            rangeNoneReverse.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }

        final TableIterator<String, FDate, Integer> range2Reverse = reverseRangeTable.rangeReverse(HASHKEY_ONE,
                twoFDate);
        Assert.assertEquals((Integer) 2, range2Reverse.next().getValue());
        Assert.assertEquals((Integer) 1, range2Reverse.next().getValue());
        Assert.assertFalse(range2Reverse.hasNext());
        try {
            range2Reverse.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }

        final TableIterator<String, FDate, Integer> range32Reverse = reverseRangeTable.rangeReverse(HASHKEY_ONE,
                threeFDate, twoFDate);
        Assert.assertEquals((Integer) 3, range32Reverse.next().getValue());
        Assert.assertEquals((Integer) 2, range32Reverse.next().getValue());
        Assert.assertFalse(range32Reverse.hasNext());
        try {
            range32Reverse.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }

        final TableIterator<String, FDate, Integer> range21Reverse = reverseRangeTable.rangeReverse(HASHKEY_ONE,
                twoFDate, oneFDate);
        Assert.assertEquals((Integer) 2, range21Reverse.next().getValue());
        Assert.assertEquals((Integer) 1, range21Reverse.next().getValue());
        Assert.assertFalse(range21Reverse.hasNext());
        try {
            range21Reverse.next();
            Assert.fail("Exception expected!");
        } catch (final NoSuchElementException e) {
            Assert.assertNotNull(e);
        }
    }

}
