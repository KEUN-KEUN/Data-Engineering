def add(a: int, b: int) -> int:
    return a + b

def test_add_success():
    assert add(1, 2) == 3
    assert add(0, 0) == 0
    assert add(-1, 5) == 4

def test_add_fail_on_type_error():
    try:
        add("1", 2)
    except TypeError:
        assert True
    else:
        assert False, "연산 타입 에러"

def test_add_fail_on_none_input():
    try:
        add(None, 2)
    except TypeError:
        assert True
    else:
        assert False, "연산 타입 에러"