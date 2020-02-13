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
import java.util.function.Function;

import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * Maps the values of the source publisher one-on-one via a mapper function.
 * <p>
 * This variant allows composing fuseable stages.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * 映射对象 将 T 类型的Flux 转换成 R 类型的 Flux
 */
final class FluxMapFuseable<T, R> extends InternalFluxOperator<T, R> implements Fuseable {

	/**
	 * 映射函数
	 */
	final Function<? super T, ? extends R> mapper;

	/**
	 * Constructs a FluxMap instance with the given source and mapper.
	 *
	 * @param source the source Publisher instance
	 * @param mapper the mapper function
	 *
	 * @throws NullPointerException if either {@code source} or {@code mapper} is null.
	 */
	FluxMapFuseable(Flux<? extends T> source,
			Function<? super T, ? extends R> mapper) {
		super(source);
		this.mapper = Objects.requireNonNull(mapper, "mapper");
	}

	/**
	 * 加工订阅者对象   这里将 R 类型 转换回  T 类型
	 * @param actual
	 * @return
	 */
	@Override
	@SuppressWarnings("unchecked")
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		if (actual instanceof ConditionalSubscriber) {
			ConditionalSubscriber<? super R> cs = (ConditionalSubscriber<? super R>) actual;
			return new MapFuseableConditionalSubscriber<>(cs, mapper);
		}
		return new MapFuseableSubscriber<>(actual, mapper);
	}

	static final class MapFuseableSubscriber<T, R>
			implements InnerOperator<T, R>,
			           QueueSubscription<R> {

		final CoreSubscriber<? super R>        actual;
		final Function<? super T, ? extends R> mapper;

		boolean done;

		QueueSubscription<T> s;

		int sourceMode;

		MapFuseableSubscriber(CoreSubscriber<? super R> actual,
				Function<? super T, ? extends R> mapper) {
			this.actual = actual;
			this.mapper = mapper;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = (QueueSubscription<T>) s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (sourceMode == ASYNC) {
				actual.onNext(null);
			}
			else {
				if (done) {
					Operators.onNextDropped(t, actual.currentContext());
					return;
				}
				R v;

				try {
					v = Objects.requireNonNull(mapper.apply(t),
							"The mapper returned a null value.");
				}
				catch (Throwable e) {
					Throwable e_ = Operators.onNextError(t, e, actual.currentContext(), s);
					if (e_ != null) {
						onError(e_);
					}
					else {
						s.request(1);
					}
					return;
				}

				actual.onNext(v);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}

			done = true;

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			actual.onComplete();
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		@Nullable
		public R poll() {
			for(;;) {
				T v = s.poll();
				if (v != null) {
					try {
						return Objects.requireNonNull(mapper.apply(v));
					}
					catch (Throwable t) {
						RuntimeException e_ = Operators.onNextPollError(v, t, currentContext());
						if (e_ != null) {
							throw e_;
						}
						else {
							continue;
						}
					}
				}
				return null;
			}
		}

		@Override
		public boolean isEmpty() {
			return s.isEmpty();
		}

		@Override
		public void clear() {
			s.clear();
		}

		@Override
		public int requestFusion(int requestedMode) {
			int m;
			if ((requestedMode & Fuseable.THREAD_BARRIER) != 0) {
				return Fuseable.NONE;
			}
			else {
				m = s.requestFusion(requestedMode);
			}
			sourceMode = m;
			return m;
		}

		@Override
		public int size() {
			return s.size();
		}
	}

	/**
	 * 订阅器 组合映射功能   就是代理模式
	 * @param <T>
	 * @param <R>
	 */
	static final class MapFuseableConditionalSubscriber<T, R>
			implements ConditionalSubscriber<T>, InnerOperator<T, R>,
			           QueueSubscription<R> {

		/**
		 * 订阅者
		 */
		final ConditionalSubscriber<? super R> actual;
		/**
		 * 映射函数
		 */
		final Function<? super T, ? extends R> mapper;

		/**
		 * 是否已经处理完内部元素
		 */
		boolean done;

		/**
		 * 该订阅者会从 subscription 对象中拉取数据
		 */
		QueueSubscription<T> s;

		int sourceMode;

		MapFuseableConditionalSubscriber(ConditionalSubscriber<? super R> actual,
				Function<? super T, ? extends R> mapper) {
			this.actual = actual;
			this.mapper = mapper;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;

			return InnerOperator.super.scanUnsafe(key);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			// 代表 初次设置 subscription 成功
			if (Operators.validate(this.s, s)) {
				this.s = (QueueSubscription<T>) s;
				actual.onSubscribe(this);
			}
		}

		/**
		 * 从上游过来的元素 会通过映射函数做处理
		 * @param t
		 */
		@Override
		public void onNext(T t) {
			// 如果数据源是异步的 通过null来触发 onNext   TODO 注意异步是怎么处理的
			if (sourceMode == ASYNC) {
				actual.onNext(null);
			}
			else {
				// done 代表 cancel 或者 处理完所有元素 这里应该是代表cancel
				if (done) {
					Operators.onNextDropped(t, actual.currentContext());
					return;
				}

				R v;

				try {
					// 使用映射函数 转换
					v = Objects.requireNonNull(mapper.apply(t),
							"The mapper returned a null value.");
				}
				catch (Throwable e) {
					Throwable e_ = Operators.onNextError(t, e, actual.currentContext(), s);
					if (e_ != null) {
						onError(e_);
					}
					else {
						s.request(1);
					}
					return;
				}

				// 使用转换后的元素 触发  onNext
				actual.onNext(v);
			}
		}

		/**
		 * 判断是否满足触发 onNext 的条件
		 * @param t the value to consume, not null
		 * @return
		 */
		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return true;
			}

			R v;

			try {
				v = Objects.requireNonNull(mapper.apply(t),
						"The mapper returned a null value.");
				return actual.tryOnNext(v);
			}
			catch (Throwable e) {
				Throwable e_ = Operators.onNextError(t, e, actual.currentContext(), s);
				if (e_ != null) {
					onError(e_);
					return true;
				}
				else {
					return false;
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}

			done = true;

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			actual.onComplete();
		}

		@Override
		public CoreSubscriber<? super R> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			// 实际上是委托 给 s内部的 subscription.request
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		@Nullable
		public R poll() {
			for(;;) {
				T v = s.poll();
				if (v != null) {
					try {
						return Objects.requireNonNull(mapper.apply(v));
					}
					catch (Throwable t) {
						RuntimeException e_ = Operators.onNextPollError(v, t, currentContext());
						if (e_ != null) {
							throw e_;
						}
						else {
							continue;
						}
					}
				}
				return null;
			}
		}

		@Override
		public boolean isEmpty() {
			return s.isEmpty();
		}

		@Override
		public void clear() {
			s.clear();
		}

		/**
		 * 获取支持的聚合模式
		 * @param requestedMode the mode requested by the intermediate operator
		 * @return
		 */
		@Override
		public int requestFusion(int requestedMode) {
			int m;
			if ((requestedMode & Fuseable.THREAD_BARRIER) != 0) {
				return Fuseable.NONE;
			}
			else {
				m = s.requestFusion(requestedMode);
			}
			sourceMode = m;
			return m;
		}

		@Override
		public int size() {
			return s.size();
		}
	}
}
