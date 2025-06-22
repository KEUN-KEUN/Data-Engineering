from unittest.mock import patch, AsyncMock

import pytest

from aysnc_lab.repository.async_lab_repository_impl import AsyncLabRepositoryImpl


# TDD (Test Driven Development)
# DDD (Domain Driven Design)
# TDD가 안되면 DDD도 잘 안됨
# Test 코드 작성하세요 <<< 필수로 시키는데 이거 작성 못하면 사실상 그냥 불합격 판정 받습니다.
@pytest.mark.asyncio
async def test_save_token_to_queue():
    repository = AsyncLabRepositoryImpl()
    test_token = "just_for_test"

    # 특정 Domain의 repository 구현체가 사용하는 asyncio.Queue()를 Mocking 하였음
    # user_token_queue가 asyncio.Queue()에 해당하며 이 녀석을 mock_queue라는 이름으로 Mocking 하여 사용
    with patch("aysnc_lab.repository.async_lab_repository_impl.user_token_queue") as mock_queue:
        mock_queue.put = AsyncMock()

        # 결국 우리가 관심이 있는 부분은 save_token_to_queue() 를 호출 했을 때
        # 실제로 내부에 있는 user_token_queue가 put() 호출을 하냐?
        # put이 동작한다면 사용자 토큰이 안정적으로 저장이 될 것이기 때문입니다.
        await repository.save_token_to_queue(test_token)

        # mock_queue는 Mocking 된 user_token_queue 였음.
        # assert_awaited_once_with() 의 경우엔 어떤 파라미터(인자) 를 가지고 실행이 되었는지 검사
        # 실제로 save_token_to_queue()를 호출하면 내부에서 user_token_queue.put()이 호출됨
        # 호출하면서 실제 사용자 토큰인 test_token을 저장하려고 합니다.
        # 그러므로 하단의 동작이 잘 이뤄졌다면 queue에는
        # 임의의 사용자 토큰이 문제 없이 잘 배치되었을 것을 보장 할 수 있습니다.
        mock_queue.put.assert_awaited_once_with(test_token)

@pytest.mark.asyncio
async def test_set_user_status():
    repository = AsyncLabRepositoryImpl()
    test_token = "just_for_test"
    status = "processing"

    # new_callable=AsyncMock을 살펴봐야함
    # 기존 동기 함수의 경우 patch를 통해 Mocking을 진행하였습니다.
    # 비동기 함수의 경우 단순 Mocking으로는 커버가 안되기 때문에
    # 이 작업들이 비동기용으로 사용하는 Mock이라는 명시적 표현으로서 AsyncMock이 필요합니다.
    with patch(
            "aysnc_lab.repository.async_lab_repository_impl.set_user_status",
            new_callable=AsyncMock
    ) as mock_set_status:
        await repository.set_user_status(test_token, status)

        mock_set_status.assert_called_once_with(test_token, status)
