from aiomysql import Pool

from typing import List, Optional

from marketing.controller.request_form.update_request_form import UpdateRequestForm
from marketing.entity.campaign_type import CampaignType
from marketing.entity.gender import Gender
from marketing.entity.marketing_data import MarketingData
from marketing.entity.user_response_type import UserResponseType
from marketing.repository.marketing_repository import MarketingRepository


class MarketingRepositoryImpl(MarketingRepository):
    def __init__(self, db_pool: Pool):
        self.dbPool = db_pool

    async def create(self, data: MarketingData) -> None:
        async with self.dbPool.acquire() as connection:
            async with connection.cursor() as cur:
                query = """
                INSERT INTO marketing_data (customer_id, age, gender, campaign_type, user_response)
                VALUES (%s, %s, %s, %s, %s)
                """
                values = (
                    data.customer_id,
                    data.age,
                    data.gender.value,
                    data.campaign_type.value,
                    data.user_response.value
                )
                await cur.execute(query, values)
                await connection.commit()

        print(f"마케팅 데이터 1개 저장 완료 (customer_id: {data.customer_id})")

    async def bulkCreate(self, data: List[MarketingData]) -> None:
        async with self.dbPool.acquire() as connection:
            async with connection.cursor() as cur:
                query = """
                INSERT INTO marketing_data (customer_id, age, gender, campaign_type, user_response)
                VALUES (%s, %s, %s, %s, %s)
                """
                values = [
                    (
                        item.customer_id,
                        item.age,
                        item.gender.value,
                        item.campaign_type.value,
                        item.user_response.value
                    )
                    for item in data
                ]
                await cur.executemany(query, values)
                await connection.commit()

        print(f"✅ {len(data)}개의 마케팅 데이터 저장 완료")

    async def findAll(self) -> List[MarketingData]:
        print("repository -> findAll()")

        async with self.dbPool.acquire() as connection:
            async with connection.cursor() as cur:
                await cur.execute("""
                    SELECT customer_id, age, gender, campaign_type, user_response 
                    FROM marketing_data
                """)
                result = await cur.fetchall()

                marketingDataList = [
                    MarketingData(
                        customer_id=row[0],
                        age=row[1],
                        gender=row[2],
                        campaign_type=row[3],
                        user_response=row[4]
                    )
                    for row in result
                ]

                return marketingDataList

    async def findById(self, id: int) -> Optional[MarketingData]:
        async with self.dbPool.acquire() as connection:
            async with connection.cursor() as cur:
                query = """
                SELECT customer_id, age, gender, campaign_type, user_response
                FROM marketing_data
                WHERE customer_id = %s
                """
                await cur.execute(query, (id,))
                row = await cur.fetchone()

                if row is None:
                    return None

                return MarketingData(
                    customer_id=row[0],
                    age=row[1],
                    gender=Gender(row[2]),
                    campaign_type=CampaignType(row[3]),
                    user_response=UserResponseType(row[4])
                )

    async def update(self, data: MarketingData) -> int:
        async with self.dbPool.acquire() as connection:
            async with connection.cursor() as cur:
                query = """
                UPDATE marketing_data
                SET age = %s,
                    gender = %s,
                    campaign_type = %s,
                    user_response = %s
                WHERE customer_id = %s
                """
                values = (
                    data.age,
                    data.gender.value,
                    data.campaign_type.value,
                    data.user_response.value,
                    data.customer_id
                )
                await cur.execute(query, values)
                await connection.commit()
                return cur.rowcount

    async def deleteById(self, customer_id: int) -> int:
        async with self.dbPool.acquire() as connection:
            async with connection.cursor() as cur:
                await cur.execute("""
                    DELETE FROM marketing_data WHERE customer_id = %s
                """, (customer_id,))
                await connection.commit()
                return cur.rowcount
