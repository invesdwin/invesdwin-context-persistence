package de.invesdwin.context.persistence.timeseries.request;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.date.FDate;

@Immutable
public abstract class ACombinedRequest<E> implements Callable<List<E>> {

    private final Callable<List<E>> higherPriority;
    private final Callable<List<E>> lowerPriority;

    public ACombinedRequest(final Callable<List<E>> higherPriority, final Callable<List<E>> lowerPriority) {
        this.higherPriority = higherPriority;
        this.lowerPriority = lowerPriority;
    }

    @Override
    public List<E> call() throws Exception {
        final List<E> higher = higherPriority.call();
        final FDate firstTimeHigher = checkAscOrder(higher);
        final List<E> lower = lowerPriority.call();
        checkAscOrder(lower);

        final List<E> combined = new ArrayList<E>();

        for (final E l : lower) {
            final FDate lTime = extractTime(l);
            if (lTime.isBefore(firstTimeHigher)) {
                combined.add(l);
            } else {
                break;
            }
        }
        combined.addAll(higher);

        return combined;
    }

    private FDate checkAscOrder(final List<E> list) {
        Assertions.checkNotEmpty(list);
        final E first = list.get(0);
        final E last = list.get(list.size() - 1);
        final FDate firstTime = extractTime(first);
        final FDate lastTime = extractTime(last);
        Assertions.assertThat(firstTime).isBeforeOrEqualTo(lastTime);
        return firstTime;
    }

    protected abstract FDate extractTime(E value);

}
