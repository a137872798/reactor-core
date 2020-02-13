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
package reactor.core.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import java.util.stream.Stream;

import reactor.core.Disposable;
import reactor.core.Scannable;

/**
 * Scheduler that hosts a fixed pool of single-threaded ScheduledExecutorService-based workers
 * and is suited for parallel work. This scheduler is time-capable (can schedule with
 * delay / periodically).
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 * 并行调度器  意思就是往该对象提交任务 会均衡的分配给 各个jdk线程池
 * 而弹性线程池是按需分配 使用会自动回收
 */
final class ParallelScheduler implements Scheduler, Supplier<ScheduledExecutorService>,
                                         Scannable {

    static final AtomicLong COUNTER = new AtomicLong();

    final int n;
    
    final ThreadFactory factory;

    volatile ScheduledExecutorService[] executors;

    /**
     * 维护全局范围的  调度器 与 定时线程池
     */
    static final AtomicReferenceFieldUpdater<ParallelScheduler, ScheduledExecutorService[]> EXECUTORS =
            AtomicReferenceFieldUpdater.newUpdater(ParallelScheduler.class, ScheduledExecutorService[].class, "executors");

    /**
     * 每个被关闭的调度器 内部的线程池会指向该对象
     */
    static final ScheduledExecutorService[] SHUTDOWN = new ScheduledExecutorService[0];

    static final ScheduledExecutorService TERMINATED;
    static {
        TERMINATED = Executors.newSingleThreadScheduledExecutor();
        TERMINATED.shutdownNow();
    }

    int roundRobin;

    /**
     * 通过并行度 和一个线程池工厂来创建调度器对象
     * @param n
     * @param factory
     */
    ParallelScheduler(int n, ThreadFactory factory) {
        if (n <= 0) {
            throw new IllegalArgumentException("n > 0 required but it was " + n);
        }
        this.n = n;
        this.factory = factory;
        init(n);
    }

    /**
     * Instantiates the default {@link ScheduledExecutorService} for the ParallelScheduler
     * ({@code Executors.newScheduledThreadPoolExecutor} with core and max pool size of 1).
     * 调度器内每个定时器对象 都是单线程的
     */
    @Override
    public ScheduledExecutorService get() {
        ScheduledThreadPoolExecutor poolExecutor = new ScheduledThreadPoolExecutor(1, factory);
        poolExecutor.setMaximumPoolSize(1);
        poolExecutor.setRemoveOnCancelPolicy(true);
        return poolExecutor;
    }

    /**
     * 调度器在初始化时 使用线程工厂来填充线程数组
     * @param n
     */
    void init(int n) {
        ScheduledExecutorService[] a = new ScheduledExecutorService[n];
        for (int i = 0; i < n; i++) {
            a[i] = Schedulers.decorateExecutorService(this, this.get());
        }
        EXECUTORS.lazySet(this, a);
    }

	@Override
	public boolean isDisposed() {
		return executors == SHUTDOWN;
	}

	@Override
    public void start() {
        ScheduledExecutorService[] b = null;
        for (;;) {
            ScheduledExecutorService[] a = executors;
            if (a != SHUTDOWN) {
                if (b != null) {
                    for (ScheduledExecutorService exec : b) {
                        exec.shutdownNow();
                    }
                }
                return;
            }

            if (b == null) {
                b = new ScheduledExecutorService[n];
                for (int i = 0; i < n; i++) {
                    b[i] = Schedulers.decorateExecutorService(this, this.get());
                }
            }
            
            if (EXECUTORS.compareAndSet(this, a, b)) {
                return;
            }
        }
    }

    @Override
    public void dispose() {
        ScheduledExecutorService[] a = executors;
        if (a != SHUTDOWN) {
            a = EXECUTORS.getAndSet(this, SHUTDOWN);
            if (a != SHUTDOWN) {
                for (ScheduledExecutorService exec : a) {
                    exec.shutdownNow();
                }
            }
        }
    }

    /**
     * 从调度器中返回一个定时器对象
     * @return
     */
    ScheduledExecutorService pick() {
        ScheduledExecutorService[] a = executors;
        if (a != SHUTDOWN) {
            // ignoring the race condition here, its already random who gets which executor
            int idx = roundRobin;
            if (idx == n) {
                idx = 0;
                roundRobin = 1;
            } else {
                roundRobin = idx + 1;
            }
            return a[idx];
        }
        return TERMINATED;
    }

    /**
     * 执行任务
     * @param task the task to execute
     *
     * @return
     */
    @Override
    public Disposable schedule(Runnable task) {
	    return Schedulers.directSchedule(pick(), task, null, 0L, TimeUnit.MILLISECONDS);
    }

    @Override
    public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
	    return Schedulers.directSchedule(pick(), task, null, delay, unit);
    }

    /**
     * 周期性执行某个任务
     * @param task the task to schedule
     * @param initialDelay the initial delay amount, non-positive values indicate non-delayed scheduling
     * @param period the period at which the task should be re-executed
     * @param unit the unit of measure of the delay amount
     * @return
     */
    @Override
    public Disposable schedulePeriodically(Runnable task,
            long initialDelay,
            long period,
            TimeUnit unit) {
	    return Schedulers.directSchedulePeriodically(pick(),
			    task,
			    initialDelay,
			    period,
			    unit);
    }

    @Override
    public String toString() {
        StringBuilder ts = new StringBuilder(Schedulers.PARALLEL)
                .append('(').append(n);
        if (factory instanceof ReactorThreadFactory) {
            ts.append(",\"").append(((ReactorThreadFactory) factory).get()).append('\"');
        }
        ts.append(')');
        return ts.toString();
    }

    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.TERMINATED || key == Attr.CANCELLED) return isDisposed();
        if (key == Attr.CAPACITY || key == Attr.BUFFERED) return n; //BUFFERED: number of workers doesn't vary
        if (key == Attr.NAME) return this.toString();

        return null;
    }

    @Override
    public Stream<? extends Scannable> inners() {
        return Stream.of(executors)
                .map(exec -> key -> Schedulers.scanExecutor(exec, key));
    }

    /**
     * 创建一个 worker 对象 该对象也具备执行任务 和 执行定时任务 的功能
     * @return
     */
    @Override
    public Worker createWorker() {
        return new ExecutorServiceWorker(pick());
    }
}
