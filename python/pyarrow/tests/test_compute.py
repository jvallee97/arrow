# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import numpy as np
import pytest

import pyarrow as pa
import pyarrow.compute as pc


all_array_types = [
    ('bool', [True, False, False, True, True]),
    ('uint8', np.arange(5)),
    ('int8', np.arange(5)),
    ('uint16', np.arange(5)),
    ('int16', np.arange(5)),
    ('uint32', np.arange(5)),
    ('int32', np.arange(5)),
    ('uint64', np.arange(5, 10)),
    ('int64', np.arange(5, 10)),
    ('float', np.arange(0, 0.5, 0.1)),
    ('double', np.arange(0, 0.5, 0.1)),
    ('string', ['a', 'b', None, 'ddd', 'ee']),
    ('binary', [b'a', b'b', b'c', b'ddd', b'ee']),
    (pa.binary(3), [b'abc', b'bcd', b'cde', b'def', b'efg']),
    (pa.list_(pa.int8()), [[1, 2], [3, 4], [5, 6], None, [9, 16]]),
    (pa.large_list(pa.int16()), [[1], [2, 3, 4], [5, 6], None, [9, 16]]),
    (pa.struct([('a', pa.int8()), ('b', pa.int8())]), [
     {'a': 1, 'b': 2}, None, {'a': 3, 'b': 4}, None, {'a': 5, 'b': 6}]),
]


numerical_arrow_types = [
    pa.int8(),
    pa.int16(),
    pa.int64(),
    pa.uint8(),
    pa.uint16(),
    pa.uint64(),
    pa.float32(),
    pa.float64()
]


@pytest.mark.parametrize('arrow_type', numerical_arrow_types)
def test_sum_array(arrow_type):
    arr = pa.array([1, 2, 3, 4], type=arrow_type)
    assert arr.sum() == 10
    assert pc.sum(arr) == 10

    arr = pa.array([], type=arrow_type)
    assert arr.sum() == None  # noqa: E711
    assert pc.sum(arr) == None  # noqa: E711


@pytest.mark.parametrize('arrow_type', numerical_arrow_types)
def test_sum_chunked_array(arrow_type):
    arr = pa.chunked_array([pa.array([1, 2, 3, 4], type=arrow_type)])
    assert pc.sum(arr) == 10

    arr = pa.chunked_array([
        pa.array([1, 2], type=arrow_type), pa.array([3, 4], type=arrow_type)
    ])
    assert pc.sum(arr) == 10

    arr = pa.chunked_array([
        pa.array([1, 2], type=arrow_type),
        pa.array([], type=arrow_type),
        pa.array([3, 4], type=arrow_type)
    ])
    assert pc.sum(arr) == 10

    arr = pa.chunked_array((), type=arrow_type)
    print(arr, type(arr))
    assert arr.num_chunks == 0
    assert pc.sum(arr) == None  # noqa: E711


def test_binary_contains_exact():
    arr = pa.array(["ab", "abc", "ba", None])
    result = pc.binary_contains_exact(arr, "ab")
    expected = pa.array([True, True, False, None])
    assert expected.equals(result)


@pytest.mark.parametrize(('ty', 'values'), all_array_types)
def test_take(ty, values):
    arr = pa.array(values, type=ty)
    for indices_type in [pa.int8(), pa.int64()]:
        indices = pa.array([0, 4, 2, None], type=indices_type)
        result = arr.take(indices)
        result.validate()
        expected = pa.array([values[0], values[4], values[2], None], type=ty)
        assert result.equals(expected)

        # empty indices
        indices = pa.array([], type=indices_type)
        result = arr.take(indices)
        result.validate()
        expected = pa.array([], type=ty)
        assert result.equals(expected)

    indices = pa.array([2, 5])
    with pytest.raises(IndexError):
        arr.take(indices)

    indices = pa.array([2, -1])
    with pytest.raises(IndexError):
        arr.take(indices)


def test_take_indices_types():
    arr = pa.array(range(5))

    for indices_type in ['uint8', 'int8', 'uint16', 'int16',
                         'uint32', 'int32', 'uint64', 'int64']:
        indices = pa.array([0, 4, 2, None], type=indices_type)
        result = arr.take(indices)
        result.validate()
        expected = pa.array([0, 4, 2, None])
        assert result.equals(expected)

    for indices_type in [pa.float32(), pa.float64()]:
        indices = pa.array([0, 4, 2], type=indices_type)
        with pytest.raises(NotImplementedError):
            arr.take(indices)


@pytest.mark.parametrize('ordered', [False, True])
def test_take_dictionary(ordered):
    arr = pa.DictionaryArray.from_arrays([0, 1, 2, 0, 1, 2], ['a', 'b', 'c'],
                                         ordered=ordered)
    result = arr.take(pa.array([0, 1, 3]))
    result.validate()
    assert result.to_pylist() == ['a', 'b', 'a']
    assert result.dictionary.to_pylist() == ['a', 'b', 'c']
    assert result.type.ordered is ordered


@pytest.mark.parametrize(('ty', 'values'), all_array_types)
def test_filter(ty, values):
    arr = pa.array(values, type=ty)

    mask = pa.array([True, False, False, True, None])
    result = arr.filter(mask, null_selection_behavior='drop')
    result.validate()
    assert result.equals(pa.array([values[0], values[3]], type=ty))
    result = arr.filter(mask, null_selection_behavior='emit_null')
    result.validate()
    assert result.equals(pa.array([values[0], values[3], None], type=ty))

    # non-boolean dtype
    mask = pa.array([0, 1, 0, 1, 0])
    with pytest.raises(NotImplementedError):
        arr.filter(mask)

    # wrong length
    mask = pa.array([True, False, True])
    with pytest.raises(ValueError, match="must all be the same length"):
        arr.filter(mask)


def test_filter_chunked_array():
    arr = pa.chunked_array([["a", None], ["c", "d", "e"]])
    expected_drop = pa.chunked_array([["a"], ["e"]])
    expected_null = pa.chunked_array([["a"], [None, "e"]])

    for mask in [
        # mask is array
        pa.array([True, False, None, False, True]),
        # mask is chunked array
        pa.chunked_array([[True, False, None], [False, True]]),
        # mask is python object
        [True, False, None, False, True]
    ]:
        result = arr.filter(mask)
        assert result.equals(expected_drop)
        result = arr.filter(mask, null_selection_behavior="emit_null")
        assert result.equals(expected_null)


def test_filter_record_batch():
    batch = pa.record_batch(
        [pa.array(["a", None, "c", "d", "e"])], names=["a'"])

    # mask is array
    mask = pa.array([True, False, None, False, True])
    result = batch.filter(mask)
    expected = pa.record_batch([pa.array(["a", "e"])], names=["a'"])
    assert result.equals(expected)

    result = batch.filter(mask, null_selection_behavior="emit_null")
    expected = pa.record_batch([pa.array(["a", None, "e"])], names=["a'"])
    assert result.equals(expected)


def test_filter_table():
    table = pa.table([pa.array(["a", None, "c", "d", "e"])], names=["a"])
    expected_drop = pa.table([pa.array(["a", "e"])], names=["a"])
    expected_null = pa.table([pa.array(["a", None, "e"])], names=["a"])

    for mask in [
        # mask is array
        pa.array([True, False, None, False, True]),
        # mask is chunked array
        pa.chunked_array([[True, False], [None, False, True]]),
        # mask is python object
        [True, False, None, False, True]
    ]:
        result = table.filter(mask)
        assert result.equals(expected_drop)
        result = table.filter(mask, null_selection_behavior="emit_null")
        assert result.equals(expected_null)


def test_filter_errors():
    arr = pa.chunked_array([["a", None], ["c", "d", "e"]])
    batch = pa.record_batch(
        [pa.array(["a", None, "c", "d", "e"])], names=["a'"])
    table = pa.table([pa.array(["a", None, "c", "d", "e"])], names=["a"])

    for obj in [arr, batch, table]:
        # non-boolean dtype
        mask = pa.array([0, 1, 0, 1, 0])
        with pytest.raises(NotImplementedError):
            obj.filter(mask)

        # wrong length
        mask = pa.array([True, False, True])
        with pytest.raises(pa.ArrowInvalid,
                           match="must all be the same length"):
            obj.filter(mask)


@pytest.mark.parametrize("typ", ["array", "chunked_array"])
def test_compare_array(typ):
    if typ == "array":
        def con(values): return pa.array(values)
    else:
        def con(values): return pa.chunked_array([values])

    arr1 = con([1, 2, 3, 4, None])
    arr2 = con([1, 1, 4, None, 4])

    result = arr1 == arr2
    assert result.equals(con([True, False, False, None, None]))

    result = arr1 != arr2
    assert result.equals(con([False, True, True, None, None]))

    result = arr1 < arr2
    assert result.equals(con([False, False, True, None, None]))

    result = arr1 <= arr2
    assert result.equals(con([True, False, True, None, None]))

    result = arr1 > arr2
    assert result.equals(con([False, True, False, None, None]))

    result = arr1 >= arr2
    assert result.equals(con([True, True, False, None, None]))


@pytest.mark.parametrize("typ", ["array", "chunked_array"])
def test_compare_scalar(typ):
    if typ == "array":
        def con(values): return pa.array(values)
    else:
        def con(values): return pa.chunked_array([values])

    arr = con([1, 2, 3, None])
    # TODO this is a hacky way to construct a scalar ..
    scalar = pa.array([2]).sum()

    result = arr == scalar
    assert result.equals(con([False, True, False, None]))

    result = arr != scalar
    assert result.equals(con([True, False, True, None]))

    result = arr < scalar
    assert result.equals(con([True, False, False, None]))

    result = arr <= scalar
    assert result.equals(con([True, True, False, None]))

    result = arr > scalar
    assert result.equals(con([False, False, True, None]))

    result = arr >= scalar
    assert result.equals(con([False, True, True, None]))


def test_compare_chunked_array_mixed():
    arr = pa.array([1, 2, 3, 4, None])
    arr_chunked = pa.chunked_array([[1, 2, 3], [4, None]])
    arr_chunked2 = pa.chunked_array([[1, 2], [3, 4, None]])

    expected = pa.chunked_array([[True, True, True, True, None]])

    for result in [
        arr == arr_chunked,
        arr_chunked == arr,
        arr_chunked == arr_chunked2,
    ]:
        assert result.equals(expected)


def test_arithmetic_add():
    left = pa.array([1, 2, 3, 4, 5])
    right = pa.array([0, -1, 1, 2, 3])
    result = pc.add(left, right)
    expected = pa.array([1, 1, 4, 6, 8])
    assert result.equals(expected)


def test_arithmetic_subtract():
    left = pa.array([1, 2, 3, 4, 5])
    right = pa.array([0, -1, 1, 2, 3])
    result = pc.subtract(left, right)
    expected = pa.array([1, 3, 2, 2, 2])
    assert result.equals(expected)


def test_arithmetic_multiply():
    left = pa.array([1, 2, 3, 4, 5])
    right = pa.array([0, -1, 1, 2, 3])
    result = pc.multiply(left, right)
    expected = pa.array([0, -2, 3, 8, 15])
    assert result.equals(expected)


def test_is_null():
    arr = pa.array([1, 2, 3, None])
    result = arr.is_null()
    result = arr.is_null()
    expected = pa.array([False, False, False, True])
    assert result.equals(expected)
    assert result.equals(pc.is_null(arr))
    result = arr.is_valid()
    expected = pa.array([True, True, True, False])
    assert result.equals(expected)
    assert result.equals(pc.is_valid(arr))

    arr = pa.chunked_array([[1, 2], [3, None]])
    result = arr.is_null()
    expected = pa.chunked_array([[False, False], [False, True]])
    assert result.equals(expected)
    result = arr.is_valid()
    expected = pa.chunked_array([[True, True], [True, False]])
    assert result.equals(expected)
