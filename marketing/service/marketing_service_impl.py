import uuid
from random import choices, randint
from fastapi import Request, HTTPException
from datetime import datetime
import json

from aiomysql import Pool

from kafka_system.constant.constant_config import ANALYSIS_REQUEST_TOPIC
from marketing.controller.request_form.update_request_form import UpdateRequestForm
from marketing.entity.marketing_data import MarketingData
from marketing.entity.campaign_type import CampaignType
from marketing.entity.gender import Gender
from marketing.entity.user_response_type import UserResponseType
from marketing.repository.marketing_repository_impl import MarketingRepositoryImpl
from marketing.service.marketing_service import MarketingService


class MarketingServiceImpl(MarketingService):
    def __init__(self, httpRequest: Request, db_pool: Pool):
        self.httpRequest = httpRequest
        self.marketingRepository = MarketingRepositoryImpl(db_pool)

    def __generateSingle(self) -> MarketingData:
        # 성별: 여성 60%, 남성 40%
        gender = choices(
            population=[Gender.female, Gender.male],
            weights=[0.6, 0.4],
            k=1
        )[0]

        # 연령: 20대 40%, 30대 40%, 그 외 20%
        age_group = choices(
            population=["20s", "30s", "other"],
            weights=[0.4, 0.4, 0.2],
            k=1
        )[0]

        if age_group == "20s":
            age = randint(20, 29)
        elif age_group == "30s":
            age = randint(30, 39)
        else:
            age = randint(18, 65)

        # 캠페인 타입: Email 고정
        campaign_type = CampaignType.email

        # 유저 응답: 무시 10%, 클릭 70%, 구매 20%
        user_response = choices(
            population=[
                UserResponseType.ignored,
                UserResponseType.clicked,
                UserResponseType.purchased
            ],
            weights=[0.1, 0.7, 0.2],
            k=1
        )[0]

        return MarketingData(
            customer_id=randint(1000, 9999),
            age=age,
            gender=gender,
            campaign_type=campaign_type,
            user_response=user_response
        )

    async def generateVirtualMarketingData(self):
        virtual_data = self.__generateSingle()
        await self.marketingRepository.create(virtual_data)
        return {"status": "success", "data": virtual_data}

    async def generateVirtualMarketingDataSet(self):
        virtual_data_list = [self.__generateSingle() for _ in range(100)]
        await self.marketingRepository.bulkCreate(virtual_data_list)
        return {"status": "success", "count": len(virtual_data_list)}

    def __serialize(self, data):
        return {
            "customer_id": data.customer_id,
            "age": data.age,
            "gender": data.gender.value,
            "campaign_type": data.campaign_type.value,
            "user_response": data.user_response.value,
        }

    async def requestAnalysis(self):
        try:
            # 1. 데이터 조회
            marketing_data_list = await self.marketingRepository.findAll()

            # 2. 직렬화
            serialized_data = [self.__serialize(data) for data in marketing_data_list]

            # 3. 메시지 구성
            request_id = f"analysis_{uuid.uuid4()}"  # UUID 생성
            analysis_message = {
                "request_id": request_id,
                "analysis_type": "CTR_CVR_SUMMARY",
                "timestamp": datetime.utcnow().isoformat(),
                "data": serialized_data
            }

            # 4. Kafka 프로듀서
            kafka_producer = self.httpRequest.app.state.kafka_producer
            await kafka_producer.send_and_wait(
                "marketing.analysis.request",
                json.dumps(analysis_message).encode("utf-8")
            )

            # # 5. Redis 상태 저장 (queued)
            # redis = self.httpRequest.app.state.redis
            # await redis.set(f"analysis_status:{request_id}", "queued")

            return {
                "success": True,
                "message": "분석 요청이 전송되었습니다.",
                "request_id": analysis_message["request_id"]
            }

        except Exception as e:
            print(f"❌ Error in requestAnalysis(): {str(e)}")
            return {
                "success": False,
                "message": "분석 요청 처리 중 오류 발생",
                "error": str(e)
            }

    async def requestDataList(self):
        dataList = await self.marketingRepository.findAll()
        return [data.to_dict() for data in dataList]

    async def readVirtualMarketingData(self, customer_id: int):
        data = await self.marketingRepository.findById(customer_id)
        if not data:
            raise ValueError(f"데이터 없음: customer_id={customer_id}")

        return {
            "customer_id": data.customer_id,
            "age": data.age,
            "gender": data.gender.value,
            "campaign_type": data.campaign_type.value,
            "user_response": data.user_response.value,
        }

    async def updateVirtualMarketingData(self, form: UpdateRequestForm) -> int:
        existing = await self.marketingRepository.findById(form.customer_id)
        if not existing:
            raise HTTPException(status_code=404, detail="해당 고객 데이터를 찾을 수 없습니다.")

        updated = MarketingData(
            customer_id=existing.customer_id,
            age=form.age if form.age is not None else existing.age,
            gender=form.gender if form.gender is not None else existing.gender,
            campaign_type=form.campaign_type if form.campaign_type is not None else existing.campaign_type,
            user_response=form.user_response if form.user_response is not None else existing.user_response
        )

        return await self.marketingRepository.update(updated)

    async def removeVirtualMarketingData(self, customer_id: int) -> int:
        existing = await self.marketingRepository.findById(customer_id)
        if not existing:
            raise HTTPException(status_code=404, detail="해당 마케팅 데이터를 찾을 수 없습니다.")
        return await self.marketingRepository.deleteById(customer_id)
