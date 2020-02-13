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

/**
 * Blocks until the upstream signals its last value or completes.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 * 阻塞直到读取到最后一个元素时 解除
 */
final class BlockingLastSubscriber<T> extends BlockingSingleSubscriber<T> {

	/**
	 * 每次触发 onNext 内部的value 都会更换  当调用 onComplete 时触发解锁
	 * @param t
	 */
	@Override
	public void onNext(T t) {
		value = t;
	}

	/**
	 * 当遇到异常时 也是直接解锁
	 * @param t
	 */
	@Override
	public void onError(Throwable t) {
		value = null;
		error = t;
		countDown();
	}
}
