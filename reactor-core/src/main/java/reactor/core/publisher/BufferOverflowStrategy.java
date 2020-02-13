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
 * Strategies to deal with overflow of a buffer during
 * {@link Flux#onBackpressureBuffer(int, BufferOverflowStrategy)
 * backpressure buffering}.
 *
 * @author Simon Baslé
 * 当背压容器溢出的时候采用的策略
 */
public enum BufferOverflowStrategy {

	/**
	 * Propagate an {@link IllegalStateException} when the buffer is full.
	 * 抛出异常
	 */
	ERROR,
	/**
	 * Drop the new element without propagating an error when the buffer is full.
	 * 丢弃最后的元素
	 */
	DROP_LATEST,
	/**
	 * When the buffer is full, remove the oldest element from it and offer the
	 * new element at the end instead. Do not propagate an error.
	 * 丢弃最早的元素
	 */
	DROP_OLDEST

}
