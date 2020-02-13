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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.util.annotation.Nullable;

/**
 * Subscribes to the source Publisher asynchronously through a scheduler function or
 * ExecutorService.
 * 
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * 使用调度器实现解耦
 */
final class FluxSubscribeOn<T> extends InternalFluxOperator<T, T> {

	final Scheduler scheduler;
	final boolean requestOnSeparateThread;

	FluxSubscribeOn(
			Flux<? extends T> source,
			Scheduler scheduler,
			boolean requestOnSeparateThread) {
		super(source);
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
		this.requestOnSeparateThread = requestOnSeparateThread;
	}

	/**
	 * 当本对象被订阅时 加工订阅者
	 * @param actual
	 * @return
	 */
	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		Worker worker;

		try {
			// 获取调度器的其中一个线程池  (根据不同实现有不同策略)
			worker = Objects.requireNonNull(scheduler.createWorker(),
					"The scheduler returned a null Function");
		} catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return null;
		}

		// 加工订阅者对象 并拉取数据
		SubscribeOnSubscriber<T> parent = new SubscribeOnSubscriber<>(source,
				actual, worker, requestOnSeparateThread);
		// 子类向上游对象拉取数据
		actual.onSubscribe(parent);

		try {
			worker.schedule(parent);
		}
		catch (RejectedExecutionException ree) {
			if (parent.s != Operators.cancelledSubscription()) {
				actual.onError(Operators.onRejectedExecution(ree, parent, null, null,
						actual.currentContext()));
			}
		}
		// 返回null 时 父类的模板中 subscribe()  就会直接返回了 也就是此时调用 subscribe() 的线程 已经可以单独执行下面的任务了
		// 订阅者会通过另一个线程 自动的获取到数据
		return null;
	}

	static final class SubscribeOnSubscriber<T>
			implements InnerOperator<T, T>, Runnable {

		/**
		 * 关联到下游
		 */
		final CoreSubscriber<? super T> actual;

		/**
		 * 关联到上游
		 */
		final CorePublisher<? extends T> source;

		final Worker  worker;
		/**
		 * 代表是否需要切换特定的线程来执行request
		 */
		final boolean requestOnSeparateThread;

		volatile Subscription s;
		static final AtomicReferenceFieldUpdater<SubscribeOnSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(SubscribeOnSubscriber.class,
						Subscription.class,
						"s");


		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SubscribeOnSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(SubscribeOnSubscriber.class,
						"requested");

		volatile Thread thread;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SubscribeOnSubscriber, Thread> THREAD =
				AtomicReferenceFieldUpdater.newUpdater(SubscribeOnSubscriber.class,
						Thread.class,
						"thread");

		/**
		 * 加工后的订阅者
		 * @param source
		 * @param actual
		 * @param worker
		 * @param requestOnSeparateThread
		 */
		SubscribeOnSubscriber(CorePublisher<? extends T> source, CoreSubscriber<? super T> actual,
				Worker worker, boolean requestOnSeparateThread) {
			this.actual = actual;
			this.worker = worker;
			this.source = source;
			this.requestOnSeparateThread = requestOnSeparateThread;
		}

		/**
		 * 当设置上游数据时  发现存在requested 那么就从上游拉取数据
		 * @param s
		 */
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				long r = REQUESTED.getAndSet(this, 0L);
				if (r != 0L) {
					requestUpstream(r, s);
				}
			}
		}

		/**
		 * 从上游对象拉取数据
		 * @param n
		 * @param s
		 */
		void requestUpstream(final long n, final Subscription s) {
			if (!requestOnSeparateThread || Thread.currentThread() == THREAD.get(this)) {
				s.request(n);
			}
			else {
				try {
					// 这里再次提交任务 也就意味着又会将任务提交到线程池  那么很可能分配到的线程已经变换了
					worker.schedule(() -> s.request(n));
				}
				catch (RejectedExecutionException ree) {
					if(!worker.isDisposed()) {
						//FIXME should not throw but if we implement strict
						// serialization like in StrictSubscriber, onNext will carry an
						// extra cost
						throw Operators.onRejectedExecution(ree, this, null, null,
								actual.currentContext());
					}
				}
			}
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			try {
				actual.onError(t);
			}
			finally {
				worker.dispose();
			}
		}

		@Override
		public void onComplete() {
			actual.onComplete();
			worker.dispose();
		}

		/**
		 * 下游申请拉取数据
		 * @param n
		 */
		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				// 如果已经设置了上游对象 就从上游开始拉取数据
				Subscription s = S.get(this);
				if (s != null) {
					requestUpstream(n, s);
				}
				else {
					Operators.addCap(REQUESTED, this, n);
					s = S.get(this);
					if (s != null) {
						long r = REQUESTED.getAndSet(this, 0L);
						if (r != 0L) {
							requestUpstream(r, s);
						}
					}

				}
			}
		}

		/**
		 * 指定当前执行任务的线程 同时促是上游发射数据
		 */
		@Override
		public void run() {
			THREAD.lazySet(this, Thread.currentThread());
			source.subscribe(this);
		}

		@Override
		public void cancel() {
			Subscription a = s;
			if (a != Operators.cancelledSubscription()) {
				a = S.getAndSet(this, Operators.cancelledSubscription());
				if (a != null && a != Operators.cancelledSubscription()) {
					a.cancel();
				}
			}
			worker.dispose();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

	}

}
