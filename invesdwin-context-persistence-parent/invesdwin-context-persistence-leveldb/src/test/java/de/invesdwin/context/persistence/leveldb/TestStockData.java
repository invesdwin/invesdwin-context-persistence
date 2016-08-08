package de.invesdwin.context.persistence.leveldb;

// CHECKSTYLE:OFF

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import com.google.common.io.CharStreams;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.time.Instant;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDateBuilder;
import ezdb.RangeTable;
import ezdb.TableIterator;
import ezdb.TableRow;

@NotThreadSafe
public class TestStockData extends ATest {

    protected static final File ROOT = ContextProperties.getHomeDirectory();

    private static final String MSFT = "MSFT";
    private static final FDate MAX_DATE = FDate.MAX_DATE;
    private static final FDate MIN_DATE = FDate.MIN_DATE;
    //    private static final FDate MAX_DATE = new GregorianCalendar(5555, 1, 1).getTime();
    //    private static final FDate MIN_DATE = new GregorianCalendar(1, 1, 1).getTime();

    protected RangeTable<String, FDate, Integer> table;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FileUtils.deleteQuietly(ROOT);

        table = new ADelegateRangeTable<String, FDate, Integer>("test") {
            @Override
            protected boolean allowHasNext() {
                return true;
            }

            @Override
            protected boolean allowPutWithoutBatch() {
                return true;
            }

        };
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        table.close();
        FileUtils.deleteQuietly(ROOT);
    }

    @Test
    public void testStockData() throws IOException, ParseException {
        InputStream in = new ClassPathResource("MSFT.txt", getClass()).getInputStream();
        final List<String> lines = CharStreams
                .readLines(new InputStreamReader(in));
        lines.remove(0);
        lines.remove(0);
        Collections.reverse(lines);
        in.close();
        Long prevLongTime = null;
        int countFDate = 0;
        FDate firstFDate = null;
        FDate lastFDate = null;
        for (final String line : lines) {
            final String[] split = line.split(",");
            Assert.assertEquals(7, split.length);
            countFDate++;
            final String dateStr = split[0];
            final FDate date = FDate.valueOf(dateStr, "yyyy-MM-dd");
            if (firstFDate == null) {
                firstFDate = date;
            }
            lastFDate = date;
            final long longTime = date.millisValue();
            if (prevLongTime != null) {
                //              System.out.println(dateStr + ":"+date + " - "+prevLongTime+"  < " + longTime + " -> "
                //                      + (prevLongTime < longTime));
                Assert.assertTrue(prevLongTime < longTime);
            }
            table.put(MSFT, date, countFDate);
            prevLongTime = longTime;
        }
        System.out.println(MSFT + " has " + countFDate + " bars");

        assertIteration(countFDate, MIN_DATE, MAX_DATE);
        assertIteration(countFDate, firstFDate, lastFDate);

        //      Fri Jan 24 23:46:40 UTC 2014
        TableIterator<String, FDate, Integer> range = table.range(MSFT, FDateBuilder.newDate(2014, 1, 23));
        int countBars = 0;
        while (range.hasNext()) {
            final TableRow<String, FDate, Integer> next = range.next();
            System.out.println(next.getValue());
            countBars++;
        }
        Assert.assertEquals(253, countBars);

        range = table.range(MSFT, FDateBuilder.newDate(2014, 1, 23), null);
        countBars = 0;
        while (range.hasNext()) {
            final TableRow<String, FDate, Integer> next = range.next();
            //          System.out.println(next.getValue());
            countBars++;
        }
        Assert.assertEquals(253, countBars);

        range = table.range(MSFT, null, FDateBuilder.newDate(1987, 1, 1));
        countBars = 0;
        while (range.hasNext()) {
            final TableRow<String, FDate, Integer> next = range.next();
            //          System.out.println(next.getValue());
            countBars++;
        }
        Assert.assertEquals(204, countBars);
    }

    private void assertIteration(final int countFDates, final FDate fromFDate, final FDate toFDate) {
        TableIterator<String, FDate, Integer> range = table.range(MSFT, fromFDate, toFDate);
        int iteratedBars = 0;
        int prevValue = 0;
        FDate left1000FDate = null;
        FDate left900FDate = null;
        final Instant start = new Instant();
        while (range.hasNext()) {
            final TableRow<String, FDate, Integer> next = range.next();
            final Integer value = next.getValue();
            // System.out.println(value);
            iteratedBars++;
            Assert.assertTrue(prevValue < value);
            prevValue = value;
            if (iteratedBars == countFDates - 999) {
                left1000FDate = next.getRangeKey();
            }
            if (iteratedBars == countFDates - 900) {
                left900FDate = next.getRangeKey();
            }
        }
        System.out.println("took: " + start);
        Assert.assertEquals(countFDates, iteratedBars);

        Assert.assertEquals((Integer) 1, table.getLatest(MSFT, fromFDate).getValue());

        Assert.assertEquals((Integer) countFDates, table.getLatest(MSFT, toFDate).getValue());

        //      System.out.println(left1000FDate +" -> "+left900FDate);
        range = table.range(MSFT, left1000FDate, left900FDate);
        int curLeftIt = 0;
        TableRow<String, FDate, Integer> prev = null;
        while (range.hasNext()) {
            final TableRow<String, FDate, Integer> next = range.next();
            curLeftIt++;
            Assert.assertEquals((Integer) (countFDates - 1000 + curLeftIt), next.getValue());
            if (prev != null) {
                final Integer nextFromPrevPlus = table.getNext(MSFT, new FDate(prev.getRangeKey().millisValue() + 1))
                        .getValue();
                Assert.assertEquals(next.getValue(), nextFromPrevPlus);
                final Integer prevFromNextMinus = table.getPrev(MSFT, new FDate(next.getRangeKey().millisValue() - 1))
                        .getValue();
                Assert.assertEquals(prev.getValue(), prevFromNextMinus);
            }
            final Integer nextFromNextIsSame = table.getNext(MSFT, new FDate(next.getRangeKey().millisValue()))
                    .getValue();
            Assert.assertEquals(next.getValue(), nextFromNextIsSame);
            final Integer prevFromNextIsSame = table.getPrev(MSFT, new FDate(next.getRangeKey().millisValue()))
                    .getValue();
            Assert.assertEquals(next.getValue(), prevFromNextIsSame);
            prev = next;
        }
        Assert.assertEquals(100, curLeftIt);
    }
}
