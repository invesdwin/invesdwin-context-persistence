package de.invesdwin.context.persistence.jpa.api;

import javax.annotation.concurrent.Immutable;

import com.mysema.query.types.Expression;
import com.mysema.query.types.OrderSpecifier;
import com.mysema.query.types.expr.BooleanExpression;
import com.mysema.query.types.expr.ComparableExpression;
import com.mysema.query.types.expr.ComparableExpressionBase;
import com.mysema.query.types.path.NumberPath;

@SuppressWarnings("rawtypes")
@Immutable
public abstract class ADelegatePath<T extends Comparable, P extends ComparableExpressionBase<T>> {

    protected final P delegate;

    private ADelegatePath(final P delegate) {
        this.delegate = delegate;
    }

    public abstract BooleanExpression loe(T right);

    public abstract BooleanExpression loe(final Expression<T> right);

    public abstract BooleanExpression goe(T right);

    public abstract BooleanExpression goe(final Expression<T> right);

    public final OrderSpecifier<T> asc() {
        return delegate.asc();
    }

    public final OrderSpecifier<T> desc() {
        return delegate.desc();
    }

    public static <A extends Number & Comparable<?>> ADelegatePath<A, NumberPath<A>> from(final NumberPath<A> path) {
        return new ADelegatePath<A, NumberPath<A>>(path) {
            @Override
            public BooleanExpression loe(final A right) {
                return delegate.loe(right);
            }

            @Override
            public BooleanExpression loe(final Expression<A> right) {
                return delegate.loe(right);
            }

            @Override
            public BooleanExpression goe(final A right) {
                return delegate.goe(right);
            }

            @Override
            public BooleanExpression goe(final Expression<A> right) {
                return delegate.goe(right);
            }
        };
    }

    public static <A extends Comparable> ADelegatePath<A, ComparableExpression<A>> from(
            final ComparableExpression<A> path) {
        return new ADelegatePath<A, ComparableExpression<A>>(path) {
            @Override
            public BooleanExpression loe(final A right) {
                return delegate.loe(right);
            }

            @Override
            public BooleanExpression loe(final Expression<A> right) {
                return delegate.loe(right);
            }

            @Override
            public BooleanExpression goe(final A right) {
                return delegate.goe(right);
            }

            @Override
            public BooleanExpression goe(final Expression<A> right) {
                return delegate.goe(right);
            }
        };
    }

}
