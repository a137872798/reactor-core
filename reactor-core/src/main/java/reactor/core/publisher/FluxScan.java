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
import java.util.function.BiFunction;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * Accumulates the source values with an accumulator function and
 * returns the intermediate results of this function.
 * <p>
 * Unlike {@link FluxScan}, this operator doesn't take an initial value
 * but treats the first source value as initial value.
 * <br>
 * The accumulation works as follows:
 * <pre><code>
 * result[0] = accumulator(source[0], source[1])
 * result[1] = accumulator(result[0], source[2])
 * result[2] = accumulator(result[1], source[3])
 * ...
 * </code></pre>
 *
 * @param <T> the input and accumulated value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * 该方法与 reduce 类似  不过reduce 是 将各个元素通过累加器计算得到结果后下发
 * 而 scan
 */
final class FluxScan<T> extends InternalFluxOperator<T, T> {

	final BiFunction<T, ? super T, T> accumulator;

	FluxScan(Flux<? extends T> source, BiFunction<T, ? super T, T> accumulator) {
		super(source);
		this.accumulator = Objects.requireNonNull(accumulator, "accumulator");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new ScanSubscriber<>(actual, accumulator);
	}

	/**
	 * 加工订阅者对象
	 * @param <T>
	 */
	static final class ScanSubscriber<T>
			implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;

		final BiFunction<T, ? super T, T> accumulator;

		Subscription s;

		T value;

		boolean done;

		ScanSubscriber(CoreSubscriber<? super T> actual, BiFunction<T, ? super T, T> accumulator) {
			this.actual = actual;
			this.accumulator = accumulator;
		}

		/**
		 * 该对象 订阅source 时触发
		 * @param s
		 */
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		/**
		 * 接收上游的数据
		 * @param t
		 */
		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			T v = value;

			if (v != null) {
				try {
					t = Objects.requireNonNull(accumulator.apply(v, t),
							"The accumulator returned a null value");
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
					return;
				}
			}
			// 下发中间结果
			value = t;
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			value = null;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			value = null;
			actual.onComplete();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.BUFFERED) return value != null ? 1 : 0;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		/**
		 * 下游对象请求拉取数据 会变成从source 拉取数据
		 * @param n
		 */
		@Override
		public void request(long n) {
			s.request(n);
		}
		
		@Override
		public void cancel() {
			s.cancel();
			//unless we want to pay a volatile, cancel() nulling out might race with onNext
			//and result in re-setting value, retaining it. So we don't null out, nor discard.
		}
	}
}
