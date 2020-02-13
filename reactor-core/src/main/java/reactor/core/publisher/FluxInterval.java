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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.util.annotation.Nullable;

/**
 * Periodically emits an ever increasing long value either via a ScheduledExecutorService
 * or a custom async callback function
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * 通过调度器对象定时往下游发送数据
 */
final class FluxInterval extends Flux<Long> implements SourceProducer<Long> {

	/**
	 * 调度器对象  每个对象内部包含一个 jdk 定时器数组
	 */
	final Scheduler timedScheduler;
	
	final long initialDelay;
	
	final long period;
	
	final TimeUnit unit;

	FluxInterval(
			long initialDelay, 
			long period, 
			TimeUnit unit, 
			Scheduler timedScheduler) {
		if (period < 0L) {
			throw new IllegalArgumentException("period >= 0 required but it was " + period);
		}
		this.initialDelay = initialDelay;
		this.period = period;
		this.unit = Objects.requireNonNull(unit, "unit");
		this.timedScheduler = Objects.requireNonNull(timedScheduler, "timedScheduler");
	}

	/**
	 * 当为该对象设置订阅者时
	 * @param actual the {@link Subscriber} interested into the published sequence
	 */
	@Override
	public void subscribe(CoreSubscriber<? super Long> actual) {
		// 生成worker 对象 该对象会维护一组任务
		Worker w = timedScheduler.createWorker();

		// 包装 subscription 对象
		IntervalRunnable r = new IntervalRunnable(actual, w);

		actual.onSubscribe(r);

		try {
			// 间接触发内部的 run()
			w.schedulePeriodically(r, initialDelay, period, unit);
		}
		catch (RejectedExecutionException ree) {
			if (!r.cancelled) {
				actual.onError(Operators.onRejectedExecution(ree, r, null, null,
						actual.currentContext()));
			}
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return timedScheduler;

		return null;
	}

	/**
	 * 该对象作为 订阅者的数据源
	 */
	static final class IntervalRunnable implements Runnable, Subscription,
	                                               InnerProducer<Long> {
		final CoreSubscriber<? super Long> actual;

		/**
		 * 该对象用于生成数据
		 */
		final Worker worker;

		/**
		 * 代表下游向上游申请了多少数据
		 */
		volatile long requested;
		static final AtomicLongFieldUpdater<IntervalRunnable> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(IntervalRunnable.class, "requested");

		/**
		 * 每次向下游发送的就是count  每次发送递增
		 */
		long count;

		volatile boolean cancelled;

		IntervalRunnable(CoreSubscriber<? super Long> actual, Worker worker) {
			this.actual = actual;
			this.worker = worker;
		}

		@Override
		public CoreSubscriber<? super Long> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.RUN_ON) return worker;

			return InnerProducer.super.scanUnsafe(key);
		}

		/**
		 * worker 执行该对象时的逻辑
		 */
		@Override
		public void run() {
			if (!cancelled) {
				// 代表下游还有请求数未消去
				if (requested != 0L) {
					actual.onNext(count++);
					if (requested != Long.MAX_VALUE) {
						REQUESTED.decrementAndGet(this);
					}
				} else {
					cancel();
					
					actual.onError(Exceptions.failWithOverflow("Could not emit tick " + count + " due to lack of requests" +
							" (interval doesn't support small downstream requests that replenish slower than the ticks)"));
				}
			}
		}
		
		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
			}
		}
		
		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				worker.dispose();
			}
		}
	}
}
