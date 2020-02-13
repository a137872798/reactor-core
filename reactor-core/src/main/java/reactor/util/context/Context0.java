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
package reactor.util.context;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * ContextX  类 都使用final 修饰 确保不会被用户篡改
 */
final class Context0 implements CoreContext {

	/**
	 * Context0 内部本身不存在任何键值对
	 */
	static final Context0 INSTANCE = new Context0();

	/**
	 * 当往context 存入key/value 后 就会进行升级
	 * @param key the key to add/update in the new {@link Context}
	 * @param value the value to associate to the key in the new {@link Context}
	 *
	 * @return
	 */
	@Override
	public Context put(Object key, Object value) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(value, "value");
		return new Context1(key, value);
	}

	@Override
	public Context delete(Object key) {
		return this;
	}

	@Override
	public <T> T get(Object key) {
		throw new NoSuchElementException("Context is empty");
	}

	@Override
	public boolean hasKey(Object key) {
		return false;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public String toString() {
		return "Context0{}";
	}

	@Override
	public Stream<Map.Entry<Object, Object>> stream() {
		return Stream.empty();
	}

	// 由于该对象本身为空 所以 put 实际上没有添加任何元素

	@Override
	public Context putAllInto(Context base) {
		return base;
	}

	@Override
	public void unsafePutAllInto(ContextN other) {
	}
}
