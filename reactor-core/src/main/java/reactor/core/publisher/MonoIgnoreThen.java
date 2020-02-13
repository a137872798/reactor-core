/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Concatenates a several Mono sources with a final Mono source by
 * ignoring values from the first set of sources and emitting the value
 * the last Mono source generates.
 *
 * @param <T> the final value type
 *           当 一组pub 全部发送完后 将last发送到下游
 */
final class MonoIgnoreThen<T> extends Mono<T> implements Fuseable, Scannable {

    final Publisher<?>[] ignore;
    
    final Mono<T> last;
    
    MonoIgnoreThen(Publisher<?>[] ignore, Mono<T> last) {
        this.ignore = Objects.requireNonNull(ignore, "ignore");
        this.last = Objects.requireNonNull(last, "last");
    }
    
    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        ThenIgnoreMain<T> manager = new ThenIgnoreMain<>(actual, ignore, last);
        actual.onSubscribe(manager);
        
        manager.drain();
    }
    
    /**
     * Shifts the current last Mono into the ignore array and sets up a new last Mono instance.
     * @param <U> the new last value type
     * @param newLast the new last Mono instance
     * @return the new operator set up
     */
    <U> MonoIgnoreThen<U> shift(Mono<U> newLast) {
        Objects.requireNonNull(newLast, "newLast");
        Publisher<?>[] a = ignore;
        int n = a.length;
        Publisher<?>[] b = new Publisher[n + 1];
        System.arraycopy(a, 0, b, 0, n);
        b[n] = last;
        
        return new MonoIgnoreThen<>(b, newLast);
    }

    @Override
    public Object scanUnsafe(Attr key) {
        return null; //no particular key to be represented, still useful in hooks
    }

    /**
     * 该对象会忽略内部的 pub 并在它们都触发onComplete 后 下发 mono的数据
     * @param <T>
     */
    static final class ThenIgnoreMain<T>
            extends Operators.MonoSubscriber<T, T> {
        final ThenIgnoreInner ignore;
        
        final ThenAcceptInner<T> accept;
        
        final Publisher<?>[] ignoreMonos;

        final Mono<T> lastMono;

        int index;
        
        volatile boolean active;
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<ThenIgnoreMain> WIP =
                AtomicIntegerFieldUpdater.newUpdater(ThenIgnoreMain.class, "wip");
        
        ThenIgnoreMain(CoreSubscriber<? super T> subscriber,
		        Publisher<?>[] ignoreMonos, Mono<T> lastMono) {
            super(subscriber);
            this.ignoreMonos = ignoreMonos;
            this.lastMono = lastMono;
            // 该对象用于消费 ignore[]
            this.ignore = new ThenIgnoreInner(this);
            // 该对象用于接收 lastMono
            this.accept = new ThenAcceptInner<>(this);
        }

	    @Override
	    public Stream<? extends Scannable> inners() {
		    return Stream.of(ignore, accept);
	    }

        /**
         * 开始拉取数据
         */
	    @SuppressWarnings("unchecked")
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            
            for (;;) {
                if (isCancelled()) {
                    return;
                }
                
                if (!active) {

                    Publisher<?>[] a = ignoreMonos;
                    int i = index;
                    // 代表 所有ignore[] 都触发了onCompelte
                    if (i == a.length) {
                        ignore.clear();
                        // 将 lastMono 的数据发送到下游
                        Mono<T> m = lastMono;
                        if (m instanceof Callable) {
                            T v;
                            try {
                                v = ((Callable<T>)m).call();
                            }
                            catch (Throwable ex) {
	                            actual.onError(Operators.onOperatorError(ex,
                                        actual.currentContext()));
	                            return;
                            }
                            
                            if (v == null) {
                                actual.onComplete();
                            }
                            else {
                                complete(v);
                            }
                            return;
                        }
                        
                        active = true;
                        m.subscribe(accept);
                    } else {
                        // 获取对应的 需要被 忽略的订阅者
                        Publisher<?> m = a[i];
                        index = i + 1;
                        
                        if (m instanceof Callable) {
                            try {
                                ((Callable<?>)m).call();
                            }
                            catch (Throwable ex) {
	                            actual.onError(Operators.onOperatorError(ex,
                                        actual.currentContext()));
	                            return;
                            }
                            
                            continue;
                        }
                        
                        active = true;
                        m.subscribe(ignore);
                    }
                }
                if (WIP.decrementAndGet(this) == 0) {
                    break;
                }
            }
        }
        
        @Override
        public void cancel() {
            super.cancel();
            ignore.cancel();
            accept.cancel();
        }

        /**
         * 代表 某个pub 的数据都发射完了
         */
        void ignoreDone() {
            active = false;
            drain();
        }
    }

    /**
     * 该对象用于 订阅内部的 ignore pub 对象
     */
    static final class ThenIgnoreInner implements InnerConsumer<Object> {
        final ThenIgnoreMain<?> parent;
        
        volatile Subscription s;
        static final AtomicReferenceFieldUpdater<ThenIgnoreInner, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(ThenIgnoreInner.class, Subscription.class, "s");
        
        ThenIgnoreInner(ThenIgnoreMain<?> parent) {
            this.parent = parent;
        }

	    @Override
        @Nullable
	    public Object scanUnsafe(Attr key) {
            if (key == Attr.PARENT) return s;
            if (key == Attr.ACTUAL) return parent;
            if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();

		    return null;
	    }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.replace(S, this, s)) {
                s.request(Long.MAX_VALUE);
            }
        }

	    @Override
	    public Context currentContext() {
		    return parent.currentContext();
	    }

        @Override
        public void onNext(Object t) {
            // ignored
            Operators.onDiscard(t, parent.currentContext()); //FIXME cache Context
        }
        
        @Override
        public void onError(Throwable t) {
            this.parent.onError(t);
        }
        
        @Override
        public void onComplete() {
            this.parent.ignoreDone();
        }
        
        void cancel() {
            Operators.terminate(S, this);
        }
        
        void clear() {
            S.lazySet(this, null);
        }
    }

    /**
     * 接收lastMono
     * @param <T>
     */
    static final class ThenAcceptInner<T> implements InnerConsumer<T> {
        final ThenIgnoreMain<T> parent;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<ThenAcceptInner, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(ThenAcceptInner.class, Subscription.class, "s");

        boolean done;
        
        ThenAcceptInner(ThenIgnoreMain<T> parent) {
            this.parent = parent;
        }

        @Override
        @Nullable
        public Object scanUnsafe(Attr key) {
            if (key == Attr.PARENT) return s;
            if (key == Attr.ACTUAL) return parent;
            if (key == Attr.TERMINATED) return done;
            if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();

            return null;
        }

        @Override
        public Context currentContext() {
            return parent.currentContext();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.setOnce(S, this, s)) {
                s.request(Long.MAX_VALUE);
            }
        }
        
        @Override
        public void onNext(T t) {
            if (done) {
                Operators.onNextDropped(t, parent.currentContext());
                return;
            }
            done = true;
            this.parent.complete(t);
        }
        
        @Override
        public void onError(Throwable t) {
            if (done) {
                Operators.onErrorDropped(t, parent.currentContext());
                return;
            }
            done = true;
            this.parent.onError(t);
        }
        
        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            this.parent.onComplete();
        }
        
        void cancel() {
            Operators.terminate(S, this);
        }
    }
}
