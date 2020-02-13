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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * An iterable that consumes a Publisher in a blocking fashion.
 * <p>
 * <p> It also implements methods to stream the contents via Stream
 * that also supports cancellation.
 *
 * @param <T> the value type
 */
final class BlockingIterable<T> implements Iterable<T>, Scannable {

    /**
     * 该对象内部维护了 publisher 也就是 Observable 代表数据源
     */
    final CorePublisher<? extends T> source;

    /**
     * 设置的 迭代器大小
     */
    final int batchSize;

    /**
     * 生成 存储元素的 队列 提供者
     */
    final Supplier<Queue<T>> queueSupplier;

    /**
     * @param source        数据源头
     * @param batchSize     容量
     * @param queueSupplier 存储队列提供者
     */
    BlockingIterable(CorePublisher<? extends T> source,
                     int batchSize,
                     Supplier<Queue<T>> queueSupplier) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize > 0 required but it was " + batchSize);
        }
        this.source = Objects.requireNonNull(source, "source");
        this.batchSize = batchSize;
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
    }

    /**
     * 查询某个状态
     *
     * @param key a {@link Attr} to resolve for the component.
     * @return
     */
    @Override
    @Nullable
    public Object scanUnsafe(Attr key) {
        if (key == Attr.PREFETCH)
            return Math.min(Integer.MAX_VALUE, batchSize); //FIXME should batchSize be forced to int?
        if (key == Attr.PARENT) return source;

        return null;
    }

    /**
     * 将订阅者 以迭代器形式返回
     *
     * @return
     */
    @Override
    public Iterator<T> iterator() {
        // 创建迭代器后 此时内部还没有元素
        SubscriberIterator<T> it = createIterator();
        // 将迭代器 作为订阅者 触发订阅
        source.subscribe(it);

        return it;
    }

    @Override
    public Spliterator<T> spliterator() {
        return stream().spliterator(); // cancellation should be composed through this way
    }

    /**
     * @return a {@link Stream} of unknown size with onClose attached to {@link
     * Subscription#cancel()}
     * 将迭代器 包装成 stream
     */
    public Stream<T> stream() {
        SubscriberIterator<T> it = createIterator();
        source.subscribe(it);

        Spliterator<T> sp = Spliterators.spliteratorUnknownSize(it, 0);

        return StreamSupport.stream(sp, false)
                .onClose(it);
    }

    /**
     * 构建 迭代器对象
     *
     * @return
     */
    SubscriberIterator<T> createIterator() {
        Queue<T> q;

        try {
            q = Objects.requireNonNull(queueSupplier.get(),
                    "The queueSupplier returned a null queue");
        } catch (Throwable e) {
            throw Exceptions.propagate(e);
        }

        // 使用生成的队列 和 长度 来创建迭代器对象
        return new SubscriberIterator<>(q, batchSize);
    }

    /**
     * 订阅者 迭代器对象
     *
     * @param <T>
     */
    static final class SubscriberIterator<T>
        // InnerConsumer 代表 内部消费者接口  是 CoreSubscriber 的子接口
            implements InnerConsumer<T>, Iterator<T>, Runnable {

        /**
         * 元素存储在该队列中
         */
        final Queue<T> queue;

        /**
         * 容量
         */
        final int batchSize;

        /**
         * 默认为 batchSize 的 3/4
         */
        final int limit;

        final Lock lock;

        final Condition condition;

        /**
         * 记录 已经生成了多少元素
         */
        long produced;

        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<SubscriberIterator, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(SubscriberIterator.class,
                        Subscription.class,
                        "s");

        volatile boolean done;
        Throwable error;

        SubscriberIterator(Queue<T> queue, int batchSize) {
            this.queue = queue;
            this.batchSize = batchSize;
            // batchSize 的 3/4
            this.limit = Operators.unboundedOrLimit(batchSize);
            this.lock = new ReentrantLock();
            this.condition = lock.newCondition();
        }

        @Override
        public Context currentContext() {
            return Context.empty();
        }

        /**
         * 判断是否还有下个元素
         * @return
         */
        @Override
        public boolean hasNext() {
            // 如果调用该方法的线程是  非阻塞线程 直接抛出异常
            if (Schedulers.isInNonBlockingThread()) {
                throw new IllegalStateException("Iterating over a toIterable() / toStream() is blocking, which is not supported in thread " + Thread.currentThread().getName());
            }
            for (; ; ) {
                boolean d = done;
                boolean empty = queue.isEmpty();
                // d == true 代表触发了 OnComplete  也就是整个数据流都已经处理完
                if (d) {
                    Throwable e = error;
                    // 抛出遇到的异常
                    if (e != null) {
                        throw Exceptions.propagate(e);
                    // 这时队列为空 就是真的处理完所有元素了
                    } else if (empty) {
                        return false;
                    }
                }
                // 如果 还没有处理过元素 且 当前队列为空 会阻塞当前线程
                if (empty) {
                    lock.lock();
                    try {
                        while (!done && queue.isEmpty()) {
                            condition.await();
                        }
                    } catch (InterruptedException ex) {
                        // run 内部会关闭 subscription
                        run();
                        throw Exceptions.propagate(ex);
                    } finally {
                        lock.unlock();
                    }
                } else {
                    return true;
                }
            }
        }

        /**
         * 获取准备好的元素  同样无法使用 NonBlocking 线程调用
         * @return
         */
        @Override
        public T next() {
            // hasNext will start by checking the thread, so `next()` would be rejected on a NONBLOCKING thread
            if (hasNext()) {
                // 从队列中弹出元素
                T v = queue.poll();

                if (v == null) {
                    run();

                    throw new IllegalStateException("Queue is empty: Expected one element to be available from the Reactive Streams source.");
                }

                long p = produced + 1;
                // 当生成的元素 达到 batchSize的  3/4时 这里会继续拉取元素  如果这时队列满了 就抛出背压队列满了的异常
                // 如果消费速度跟的上 且 元素还没消费完 就会不断的 拉取元素  因为一开始不确定 subscription 内部共有多少元素
                // 消费端过快 就会阻塞消费线程  生产端过快 这里会产生背压异常
                if (p == limit) {
                    produced = 0;
                    s.request(p);
                } else {
                    produced = p;
                }

                return v;
            }
            throw new NoSuchElementException();
        }

        /**
         * 该 subscriber (SubscriberIterator) 订阅相关的方法
         * @param s
         */
        @Override
        public void onSubscribe(Subscription s) {
            if (Operators.setOnce(S, this, s)) {
                // 初始化 成功时 拉取batch 大小的元素
                s.request(Operators.unboundedOrPrefetch(batchSize));
            }
        }

        /**
         * 当成功拉取到一个元素 并进行处理时
         * @param t
         */
        @Override
        public void onNext(T t) {
            // 代表队列满了
            if (!queue.offer(t)) {
                // 这里会关闭掉 subscription
                Operators.terminate(S, this);

                // 生成一个 背压相关的异常
                onError(Operators.onOperatorError(null,
                        Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
                        t, currentContext()));
            } else {
                // 一旦消费到元素后 就解除阻塞状态  这样某条阻塞的线程 就可以成功调用 hasNext
                signalConsumer();
            }
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            done = true;
            signalConsumer();
        }

        @Override
        public void onComplete() {
            done = true;
            signalConsumer();
        }

        void signalConsumer() {
            lock.lock();
            try {
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }

        /**
         * 关闭 subscription
         */
        @Override
        public void run() {
            Operators.terminate(S, this);
            signalConsumer();
        }

        @Override
        @Nullable
        public Object scanUnsafe(Attr key) {
            if (key == Attr.TERMINATED) return done;
            if (key == Attr.PARENT) return s;
            if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
            if (key == Attr.PREFETCH) return batchSize;
            if (key == Attr.ERROR) return error;

            return null;
        }
    }

}
