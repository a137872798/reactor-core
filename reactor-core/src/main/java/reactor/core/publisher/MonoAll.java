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
import java.util.function.Predicate;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * Emits a single boolean true if all values of the source sequence match
 * the predicate.
 * <p>
 * The implementation uses short-circuit logic and completes with false if
 * the predicate doesn't match a value.
 *
 * @param <T> the source value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * 计算谓语的结果 并传播到下游
 */
final class MonoAll<T> extends MonoFromFluxOperator<T, Boolean>
		implements Fuseable {

	final Predicate<? super T> predicate;

	MonoAll(Flux<? extends T> source, Predicate<? super T> predicate) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	/**
	 * 当该对象设置订阅者时 包装订阅者  使得上游订阅该对象
	 * @param actual
	 * @return
	 */
	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super Boolean> actual) {
		return new AllSubscriber<T>(actual, predicate);
	}

	/**
	 * 当上游订阅该对象时  又将该对象 与上游数据包装成 subscription 并设置到 该对象的 s 字段中 注意上游数据是 flux 因为MonoAll是将
	 * flux 适配成了 mono
	 * @param <T>
	 */
	static final class AllSubscriber<T> extends Operators.MonoSubscriber<T, Boolean> {
		final Predicate<? super T> predicate;

		/**
		 * 包含本对象 已经上游的 flux 对象
		 */
		Subscription s;

		/**
		 * 是否已经处理完毕
		 */
		boolean done;

		AllSubscriber(CoreSubscriber<? super Boolean> actual, Predicate<? super T> predicate) {
			super(actual);
			this.predicate = predicate;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PARENT) return s;

			return super.scanUnsafe(key);
		}

		@Override
		public void cancel() {
			s.cancel();
			super.cancel();
		}

		/**
		 * 为该对象设置订阅者
		 * @param s
		 */
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				// 通过实际的订阅者 触发该对象的 request 方法
				actual.onSubscribe(this);

				// 该方法会触发 flux 的 request 并接收上游数据 之后触发onNext
				s.request(Long.MAX_VALUE);
			}
		}

		/**
		 * 接收到上游的数据
		 */
		@Override
		public void onNext(T t) {

			// 代表 关闭 或者已经处理过数据了  先忽略
			if (done) {
				return;
			}

			boolean b;

			try {
				// 通过谓语对象 生成结果  看来该方法会接收 flux 的所有数据 并通过该谓语对象去判断 只要有一个不符合条件 就不满足 all的条件
				b = predicate.test(t);
			} catch (Throwable e) {
				done = true;
				actual.onError(Operators.onOperatorError(s, e, t, actual.currentContext()));
				return;
			}
			// 代表上游的 flux 没有全部满足条件
			if (!b) {
				done = true;
				s.cancel();

				// 触发函数
				complete(false);
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

		// 代表 上游所有数据 都满足条件
		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			complete(true);
		}

	}
}
