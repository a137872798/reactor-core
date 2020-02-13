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

package reactor.core;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.util.context.Context;

/**
 * A {@link Context} aware subscriber which has relaxed rules for §1.3 and §3.9
 * compared to the original {@link org.reactivestreams.Subscriber} from Reactive Streams.
 * If an invalid request {@code <= 0} is done on the received subscription, the request
 * will not produce an onError and will simply be ignored.
 *
 * <p>
 * The rule relaxation has been initially established under reactive-streams-commons.
 *
 * @param <T> the {@link Subscriber} data type
 *
 * @since 3.1.0
 * 该接口用于拓展订阅者
 */
public interface CoreSubscriber<T> extends Subscriber<T> {

	/**
	 * Request a {@link Context} from dependent components which can include downstream
	 * operators during subscribing or a terminal {@link org.reactivestreams.Subscriber}.
	 *
	 * @return a resolved context or {@link Context#empty()}
	 * 获取本次响应式处理中的 上下文 实际上内部就是维护键值对   默认情况下返回empty() 代表内部部包含任何键值对
	 */
	default Context currentContext(){
		return Context.empty();
	}

	/**
	 * Implementors should initialize any state used by {@link #onNext(Object)} before
	 * calling {@link Subscription#request(long)}. Should further {@code onNext} related
	 * state modification occur, thread-safety will be required.
	 * <p>
	 *    Note that an invalid request {@code <= 0} will not produce an onError and
	 *    will simply be ignored or reported through a debug-enabled
	 *    {@link reactor.util.Logger}.
	 *
	 * {@inheritDoc}
	 * subscription 内部包含了 observable 内部的数据
	 */
	@Override
	void onSubscribe(Subscription s);
}
