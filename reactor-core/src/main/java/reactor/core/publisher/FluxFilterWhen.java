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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;


/**
 * Maps each upstream value into a single {@code true} or {@code false} value provided by
 * a generated Publisher for that input value and emits the input value if the inner
 * Publisher returned {@code true}.
 * <p>
 * Only the first item emitted by the inner Publisher's are considered. If
 * the inner Publisher is empty, no resulting item is generated for that input value.
 *
 * @param <T> the input value type
 *
 * @author David Karnok
 * @author Simon Baslé
 */
//adapted from RxJava2Extensions: https://github.com/akarnokd/RxJava2Extensions/blob/master/src/main/java/hu/akarnokd/rxjava2/operators/FlowableFilterAsync.java
class FluxFilterWhen<T> extends InternalFluxOperator<T, T> {

	/**
	 * 触发 onNext 的对象会通过该谓语
	 */
	final Function<? super T, ? extends Publisher<Boolean>> asyncPredicate;

	/**
	 * 代表存储元素的队列大小
	 */
	final int bufferSize;

	FluxFilterWhen(Flux<T> source,
			Function<? super T, ? extends Publisher<Boolean>> asyncPredicate,
			int bufferSize) {
		super(source);
		this.asyncPredicate = asyncPredicate;
		this.bufferSize = bufferSize;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new FluxFilterWhenSubscriber<>(actual, asyncPredicate, bufferSize);
	}

	/**
	 * 包装订阅者对象
	 * @param <T>
	 */
	static final class FluxFilterWhenSubscriber<T> implements InnerOperator<T, T> {

		final Function<? super T, ? extends Publisher<Boolean>> asyncPredicate;
		final int                                               bufferSize;
		final AtomicReferenceArray<T>                           toFilter;
		final CoreSubscriber<? super T>                         actual;
		final Context                                           ctx;

		int          consumed;
		long         consumerIndex;
		long         emitted;
		/**
		 * 对用 current 的结果
		 */
		Boolean      innerResult;
		long         producerIndex;
		Subscription upstream;

		volatile boolean         cancelled;
		volatile FilterWhenInner current;
		volatile boolean         done;
		volatile Throwable       error;
		volatile long            requested;
		volatile int             state;
		volatile int             wip;

		static final AtomicReferenceFieldUpdater<FluxFilterWhenSubscriber, Throwable>      ERROR     =
				AtomicReferenceFieldUpdater.newUpdater(FluxFilterWhenSubscriber.class, Throwable.class, "error");
		static final AtomicLongFieldUpdater<FluxFilterWhenSubscriber>                      REQUESTED =
				AtomicLongFieldUpdater.newUpdater(FluxFilterWhenSubscriber.class, "requested");
		static final AtomicIntegerFieldUpdater<FluxFilterWhenSubscriber>                   WIP       =
				AtomicIntegerFieldUpdater.newUpdater(FluxFilterWhenSubscriber.class, "wip");
		static final AtomicReferenceFieldUpdater<FluxFilterWhenSubscriber, FilterWhenInner>CURRENT   =
				AtomicReferenceFieldUpdater.newUpdater(FluxFilterWhenSubscriber.class, FilterWhenInner.class, "current");

		@SuppressWarnings("ConstantConditions")
		static final FilterWhenInner INNER_CANCELLED = new FilterWhenInner(null, false);

		/**
		 * 代表初始阶段
		 */
		static final int STATE_FRESH   = 0;
		/**
		 * 代表已经为  某个数据生成 Pub<Boolean> 对象
		 */
		static final int STATE_RUNNING = 1;
		/**
		 * 代表 Pub<Boolean> 对象已经下发结果
		 */
		static final int STATE_RESULT  = 2;

		FluxFilterWhenSubscriber(CoreSubscriber<? super T> actual,
				Function<? super T, ? extends Publisher<Boolean>> asyncPredicate,
				int bufferSize) {
			this.actual = actual;
			this.ctx = actual.currentContext();
			this.toFilter = new AtomicReferenceArray<>(Queues.ceilingNextPowerOfTwo(bufferSize));
			this.asyncPredicate = asyncPredicate;
			this.bufferSize = bufferSize;
		}

		@Override
		public final CoreSubscriber<? super T> actual() {
			return actual;
		}

		/**
		 * 上游传播数据到 转发对象
		 * @param t
		 */
		@Override
		public void onNext(T t) {
			long pi = producerIndex;
			int m = toFilter.length() - 1;

			// 获取当前偏移量   注意这是一个wheel
			int offset = (int)pi & m;
			// 更新数组
			toFilter.lazySet(offset, t);
			producerIndex = pi + 1;
			drain();
		}

		@Override
		public void onError(Throwable t) {
			ERROR.set(this, t);
			done = true;
			drain();
		}

		/**
		 * 代表上游的所有数据都已经下发
		 */
		@Override
		public void onComplete() {
			done = true;
			drain();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				drain();
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				upstream.cancel();
				cancelInner();
				if (WIP.getAndIncrement(this) == 0) {
					clear();
				}
			}
		}

		void cancelInner() {
			FilterWhenInner a = CURRENT.get(this);
			if (a != INNER_CANCELLED) {
				a = CURRENT.getAndSet(this, INNER_CANCELLED);
				if (a != null && a != INNER_CANCELLED) {
					a.cancel();
				}
			}
		}

		void clear() {
			int n = toFilter.length();
			for (int i = 0; i < n; i++) {
				T old = toFilter.getAndSet(i, null);
				Operators.onDiscard(old, ctx);
			}
			innerResult = null;
		}

		/**
		 * 首先触发该方法  设置上游对象
		 * @param s
		 */
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(upstream, s)) {
				upstream = s;
				// 连接到下游对象 并触发 request方法
				actual.onSubscribe(this);
				s.request(bufferSize);
			}
		}

		/**
		 * 尝试拉取数据
		 */
		@SuppressWarnings("unchecked")
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;
			int limit = Operators.unboundedOrLimit(bufferSize);
			long e = emitted;
			long ci = consumerIndex;
			int f = consumed;
			int m = toFilter.length() - 1;
			Subscriber<? super T> a = actual;

			for (;;) {
				long r = requested;

				while (e != r) {
					if (cancelled) {
						clear();
						return;
					}

					boolean d = done;

					int offset = (int)ci & m;
					// 获取原子数据对应的数据
					T t = toFilter.get(offset);
					boolean empty = t == null;

					if (d && empty) {
						Throwable ex = Exceptions.terminate(ERROR, this);
						if (ex == null) {
							a.onComplete();
						} else {
							a.onError(ex);
						}
						return;
					}

					if (empty) {
						break;
					}

					int s = state;
					// 代表 初始状态  或者某个谓语结果刚刚被消耗
					if (s == STATE_FRESH) {
						Publisher<Boolean> p;

						// 使用谓语对象处理元素  生成一个 pub 该对象会往下发一个 boolean 代表本次谓语处理的结果
						try {
							p = Objects.requireNonNull(asyncPredicate.apply(t), "The asyncPredicate returned a null value");
						} catch (Throwable ex) {
							Exceptions.throwIfFatal(ex);
							Exceptions.addThrowable(ERROR, this, ex);
							p = null; //discarded as "old" below
						}

						if (p != null) {
							if (p instanceof Callable) {
								Boolean u;

								try {
									u = ((Callable<Boolean>)p).call();
								} catch (Throwable ex) {
									Exceptions.throwIfFatal(ex);
									Exceptions.addThrowable(ERROR, this, ex);
									u = null; //triggers discard below
								}

								if (u != null && u) {
									a.onNext(t);
									e++;
								}
								else {
									Operators.onDiscard(t, ctx);
								}
							} else {
								// 创建一个 inner 对象 并订阅用谓语产生的pub对象
								FilterWhenInner inner = new FilterWhenInner(this, !(p instanceof Mono));
								// innerResult 的结果对应 current
								// 如果 上个元素已经抢占了位置 那么 current 已经被设置好了  那么只能尝试将元素存放到数组中 等待 上一个pub先发射数据
								if (CURRENT.compareAndSet(this,null, inner)) {
									state = STATE_RUNNING;
									p.subscribe(inner);
									break;
								}
							}
						}

						// 代表 current 被其他线程抢占了 丢弃该元素
						T old = toFilter.getAndSet(offset, null);
						Operators.onDiscard(old, ctx);
						ci++;
						// 增加 consumer 相关的偏移量  当达到limit 时 申请下一批数据
						if (++f == limit) {
							f = 0;
							upstream.request(limit);
						}
						// 如果刚好有个 pub 生成了结果
					} else
					if (s == STATE_RESULT) {
						Boolean u = innerResult;
						innerResult = null;

						// 根据该结果来判断是否要将数据下发
						if (u != null && u) {
							a.onNext(t);
							e++;
						}
						else {
							Operators.onDiscard(t, ctx);
						}

						toFilter.lazySet(offset, null);
						ci++;
						if (++f == limit) {
							f = 0;
							upstream.request(limit);
						}
						state = STATE_FRESH;
					} else {
						break;
					}
				}

				if (e == r) {
					if (cancelled) {
						clear();
						return;
					}

					boolean d = done;

					int offset = (int)ci & m;
					T t = toFilter.get(offset);
					boolean empty = t == null;

					if (d && empty) {
						Throwable ex = Exceptions.terminate(ERROR, this);
						if (ex == null) {
							a.onComplete();
						} else {
							a.onError(ex);
						}
						return;
					}
				}

				int w = wip;
				if (missed == w) {
					consumed = f;
					consumerIndex = ci;
					emitted = e;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				} else {
					missed = w;
				}
			}
		}

		void clearCurrent() {
			FilterWhenInner c = current;
			if (c != INNER_CANCELLED) {
				CURRENT.compareAndSet(this, c, null);
			}
		}

		/**
		 * 代表某个数据通过谓语并产生了结果
		 * @param item
		 */
		void innerResult(Boolean item) {
			innerResult = item;
			state = STATE_RESULT;
			// 清除掉当前的inner 对象
			clearCurrent();
			drain();
		}

		void innerError(Throwable ex) {
			Exceptions.addThrowable(ERROR, this, ex);
			state = STATE_RESULT;
			clearCurrent();
			drain();
		}

		void innerComplete() {
			state = STATE_RESULT;
			clearCurrent();
			drain();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return upstream;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.ERROR) //FIXME ERROR is often reset by Exceptions.terminate :(
				return error;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.CAPACITY) return toFilter.length();
			if (key == Attr.LARGE_BUFFERED) return producerIndex - consumerIndex;
			if (key == Attr.BUFFERED) {
				long realBuffered = producerIndex - consumerIndex;
				if (realBuffered <= Integer.MAX_VALUE) return (int) realBuffered;
				return Integer.MIN_VALUE;
			}
			if (key == Attr.PREFETCH) return bufferSize;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			FilterWhenInner c = current;
			return c == null ? Stream.empty() : Stream.of(c);
		}
	}

	/**
	 * 该对象用于订阅 谓语产生的结果
	 */
	static final class FilterWhenInner implements InnerConsumer<Boolean> {

		final FluxFilterWhenSubscriber<?> parent;
		final boolean                     cancelOnNext;

		boolean done;

		volatile Subscription sub;

		static final AtomicReferenceFieldUpdater<FilterWhenInner, Subscription> SUB =
				AtomicReferenceFieldUpdater.newUpdater(FilterWhenInner.class, Subscription.class, "sub");

		FilterWhenInner(FluxFilterWhenSubscriber<?> parent, boolean cancelOnNext) {
			this.parent = parent;
			this.cancelOnNext = cancelOnNext;
		}

		@Override
		public Context currentContext() {
			return parent.currentContext();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(SUB, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		/**
		 * 将谓语生成的结果传播到下游
		 * @param t
		 */
		@Override
		public void onNext(Boolean t) {
			if (!done) {
				if (cancelOnNext) {
					sub.cancel();
				}
				done = true;
				parent.innerResult(t);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (!done) {
				done = true;
				parent.innerError(t);
			} else {
				Operators.onErrorDropped(t, parent.currentContext());
			}
		}

		/**
		 * 代表对应的 Pub<Boolean> 数据已经发射完
		 */
		@Override
		public void onComplete() {
			if (!done) {
				done = true;
				parent.innerComplete();
			}
		}

		void cancel() {
			Operators.terminate(SUB, this);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return parent;
			if (key == Attr.ACTUAL) return sub;
			if (key == Attr.CANCELLED) return sub == Operators.cancelledSubscription();
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return done ? 0L : 1L;

			return null;
		}
	}
}
