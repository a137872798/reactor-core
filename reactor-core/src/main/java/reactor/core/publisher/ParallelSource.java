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

import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Dispatches the values from upstream in a round robin fashion to subscribers which are
 * ready to consume elements. A value from upstream is sent to only one of the subscribers.
 *
 * @param <T> the value type
 *           并行 flux
 *           实际上本对象并没有借助多线程  而是以轮询的方式 挨个往下游每个订阅者发送数据
 */
final class ParallelSource<T> extends ParallelFlux<T> implements Scannable {

	/**
	 * 数据源
	 */
	final Publisher<? extends T> source;
	
	final int parallelism;
	
	final int prefetch;
	
	final Supplier<Queue<T>> queueSupplier;

	ParallelSource(Publisher<? extends T> source, int parallelism, int prefetch, Supplier<Queue<T>> queueSupplier) {
		if (parallelism <= 0) {
			throw new IllegalArgumentException("parallelism > 0 required but it was " + parallelism);
		}
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.source = source;
		this.parallelism = parallelism;
		this.prefetch = prefetch;
		this.queueSupplier = queueSupplier;
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	public int parallelism() {
		return parallelism;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Scannable.Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.PREFETCH) return getPrefetch();

		return null;
	}


	/**
	 * 为该对象设置订阅者     注意并行流对象会设置一个订阅者数据 而且数量跟并行度一致
	 * @param subscribers the subscribers array to run in parallel, the number of items
	 *                    也就是 每次为并行流设置订阅者时  总是一个 跟并行度相同的数组
	 *
	 */
	@Override
	public void subscribe(CoreSubscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		source.subscribe(new ParallelSourceMain<>(subscribers, prefetch, queueSupplier));
	}


	/**
	 * 包装订阅者对象  内部可以包含多个订阅者
	 * @param <T>
	 */
	static final class ParallelSourceMain<T> implements InnerConsumer<T> {

		/**
		 * 一组订阅者对象
		 */
		final CoreSubscriber<? super T>[] subscribers;

		/**
		 * 对应 subscribers 的request 数量
		 */
		final AtomicLongArray requests;

		/**
		 * 为每个 subscribers 发射的数量
		 */
		final long[] emissions;

		final int prefetch;
		
		final int limit;

		final Supplier<Queue<T>> queueSupplier;

		Subscription s;
		
		Queue<T> queue;
		
		Throwable error;
		
		volatile boolean done;
		
		int index;
		
		volatile boolean cancelled;
		
		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ParallelSourceMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ParallelSourceMain.class, "wip");
		
		/** 
		 * Counts how many subscribers were setup to delay triggering the
		 * drain of upstream until all of them have been setup.
		 */
		volatile int subscriberCount;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ParallelSourceMain> SUBSCRIBER_COUNT =
				AtomicIntegerFieldUpdater.newUpdater(ParallelSourceMain.class, "subscriberCount");
		
		int produced;
		
		int sourceMode;

		ParallelSourceMain(CoreSubscriber<? super T>[] subscribers, int prefetch,
				Supplier<Queue<T>> queueSupplier) {
			this.subscribers = subscribers;
			this.prefetch = prefetch;
			this.queueSupplier = queueSupplier;
			this.limit = Operators.unboundedOrLimit(prefetch);
			this.requests = new AtomicLongArray(subscribers.length);
			this.emissions = new long[subscribers.length];
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.ERROR) return error;
			if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;

			return null;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(subscribers).map(Scannable::from);
		}

		@Override
		public Context currentContext() {
			return subscribers[0].currentContext();
		}

		/**
		 * 先触发该方法 将上游数据与本对象 整合成 onSubscribe
		 * @param s
		 */
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				if (s instanceof Fuseable.QueueSubscription) {
					@SuppressWarnings("unchecked")
					Fuseable.QueueSubscription<T> qs = (Fuseable.QueueSubscription<T>) s;
					
					int m = qs.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);
					
					if (m == Fuseable.SYNC) {
						sourceMode = m;
						queue = qs;
						done = true;
						setupSubscribers();
						drain();
						return;
					} else
					if (m == Fuseable.ASYNC) {
						sourceMode = m;
						queue = qs;
						
						setupSubscribers();
						
						s.request(Operators.unboundedOrPrefetch(prefetch));
						
						return;
					}
				}
				
				queue = queueSupplier.get();

				// 为订阅者生成对应的inner 对象  并向本对象拉取数据 (触发drain())
				setupSubscribers();

				// 向上游拉取数据
				s.request(Operators.unboundedOrPrefetch(prefetch));
			}
		}

		/**
		 * 设置订阅者
		 */
		void setupSubscribers() {
			int m = subscribers.length;
			
			for (int i = 0; i < m; i++) {
				if (cancelled) {
					return;
				}
				int j = i;

				// 记录当前订阅者数量
				SUBSCRIBER_COUNT.lazySet(this, i + 1);

				// 每个订阅者 单独订阅一个inner 对象  这样上游数据 会转发到inner 之后通过inner 转发到下游
				subscribers[i].onSubscribe(new ParallelSourceInner<>(this, j, m));
			}
		}

		/**
		 * 代表上游将数据下发
		 * @param t
		 */
		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, currentContext());
				return;
			}
			// 将数据存储到 queue 中
			if (sourceMode == Fuseable.NONE) {
				if (!queue.offer(t)) {
					onError(Operators.onOperatorError(s, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), t, currentContext()));
					return;
				}
			}
			// 将数据转发到inner 中
			drain();
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, currentContext());
				return;
			}
			error = t;
			done = true;
			drain();
		}

		@Override
		public void onComplete() {
			if(done){
				return;
			}
			done = true;
			drain();
		}
		
		void cancel() {
			if (!cancelled) {
				cancelled = true;
				this.s.cancel();
				
				if (WIP.getAndIncrement(this) == 0) {
					queue.clear();
				}
			}
		}
		
		void drainAsync() {
			int missed = 1;
			
			Queue<T> q = queue;
			CoreSubscriber<? super T>[] a = this.subscribers;
			AtomicLongArray r = this.requests;
			long[] e = this.emissions;
			int n = e.length;
			// 代表当前正在处理的 订阅者
			int idx = index;
			int consumed = produced;
			
			for (;;) {

				int notReady = 0;
				
				for (;;) {
					if (cancelled) {
						q.clear();
						return;
					}
					
					boolean d = done;
					if (d) {
						Throwable ex = error;
						if (ex != null) {
							q.clear();
							for (Subscriber<? super T> s : a) {
								s.onError(ex);
							}
							return;
						}
					}

					boolean empty = q.isEmpty();

					// 这里队列中没有元素不会提前退出
					if (d && empty) {
						for (Subscriber<? super T> s : a) {
							s.onComplete();
						}
						return;
					}

					if (empty) {
						break;
					}

					// 如果没有切换线程(单线程运行这个模板 看上去就像是轮询往下游发送数据)
					long ridx = r.get(idx);
					long eidx = e[idx];
					if (ridx != eidx) {

						T v;
						
						try {
							v = q.poll();
						} catch (Throwable ex) {
							ex = Operators.onOperatorError(s, ex, a[idx].currentContext());
							for (Subscriber<? super T> s : a) {
								s.onError(ex);
							}
							return;
						}
						
						if (v == null) {
							break;
						}
						
						a[idx].onNext(v);
						
						e[idx] = eidx + 1;
						
						int c = ++consumed;
						if (c == limit) {
							consumed = 0;
							s.request(c);
						}
						notReady = 0;
					} else {
						notReady++;
					}

					// 每次接收一个元素后 自动将数据发往下一个订阅者
					idx++;
					if (idx == n) {
						idx = 0;
					}
					
					if (notReady == n) {
						break;
					}
				}
				
				int w = wip;
				if (w == missed) {
					index = idx;
					produced = consumed;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				} else {
					missed = w;
				}
			}
		}

		/**
		 * 代表同步模式下转发
		 */
		void drainSync() {
			int missed = 1;
			
			Queue<T> q = queue;
			CoreSubscriber<? super T>[] a = this.subscribers;
			AtomicLongArray r = this.requests;
			long[] e = this.emissions;
			int n = e.length;
			int idx = index;
			
			for (;;) {

				int notReady = 0;
				
				for (;;) {
					if (cancelled) {
						q.clear();
						return;
					}

					// 在同步模式下 只要上游没有准备好数据 这里就直接触发 onComplete
					if (q.isEmpty()) {
						for (Subscriber<? super T> s : a) {
							s.onComplete();
						}
						return;
					}

					// 这里要开始将数据发往下游了
					long ridx = r.get(idx);
					long eidx = e[idx];
					// 代表request > emission

					if (ridx != eidx) {

						T v;
						
						try {
							// 从队列中拉出数据
							v = q.poll();
						} catch (Throwable ex) {
							ex = Operators.onOperatorError(s, ex, a[idx].currentContext());
							for (Subscriber<? super T> s : a) {
								s.onError(ex);
							}
							return;
						}
						
						if (v == null) {
							for (Subscriber<? super T> s : a) {
								s.onComplete();
							}
							return;
						}
						
						a[idx].onNext(v);
						
						e[idx] = eidx + 1;
						
						notReady = 0;
					} else {
						notReady++;
					}
					
					idx++;
					if (idx == n) {
						idx = 0;
					}
					
					if (notReady == n) {
						break;
					}
				}
				
				int w = wip;
				if (w == missed) {
					index = idx;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				} else {
					missed = w;
				}
			}
		}

		/**
		 * 将上游数据转发到下游
		 */
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			
			if (sourceMode == Fuseable.SYNC) {
				drainSync();
			} else {
				// 默认情况应该是这个
				drainAsync();
			}
		}

		/**
		 * 根据下标和总数来生成 inner 对象 负责往 subscribe 发送数据
		 * @param <T>
		 */
		static final class ParallelSourceInner<T> implements InnerProducer<T> {

			final ParallelSourceMain<T> parent;

			/**
			 * 代表是 parent 的第几个订阅者
			 */
			final int index;
			/**
			 * 总共有多少订阅者
			 */
			final int length;

			ParallelSourceInner(ParallelSourceMain<T> parent, int index, int length) {
				this.index = index;
				this.length = length;
				this.parent = parent;
			}

			@Override
			public CoreSubscriber<? super T> actual() {
				return parent.subscribers[index];
			}

			@Override
			@Nullable
			public Object scanUnsafe(Attr key) {
				if (key == Attr.PARENT) return parent;

				return InnerProducer.super.scanUnsafe(key);
			}

			/**
			 * 当下游 实际的订阅者向本对象拉取数据时
			 * @param n
			 */
			@Override
			public void request(long n) {
				if (Operators.validate(n)) {
					AtomicLongArray ra = parent.requests;
					for (;;) {
						// 找到对应的 request slot 并增加值
						long r = ra.get(index);
						if (r == Long.MAX_VALUE) {
							return;
						}
						long u = Operators.addCap(r, n);
						if (ra.compareAndSet(index, r, u)) {
							break;
						}
					}
					// 代表所有 inner 对象都生成后 让上游发派数据
					if (parent.subscriberCount == length) {
						parent.drain();
					}
				}
			}

			@Override
			public void cancel() {
				parent.cancel();
			}
		}
	}
}
