from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.responses import JSONResponse

from aiomysql import Pool

from async_db.database import getMySqlPool
from marketing.service.marketing_service_impl import MarketingServiceImpl

marketingRouter = APIRouter()

# 의존성 주입
async def injectMarketingService(httpRequest: Request, db_pool: Pool = Depends(getMySqlPool)) -> MarketingServiceImpl:
    return MarketingServiceImpl(httpRequest, db_pool)

@marketingRouter.post("/marketing/create-virtual-data-set")
async def generateVirtualMarketingData(
    marketingService: MarketingServiceImpl = Depends(injectMarketingService)
):
    try:
        await marketingService.generateVirtualMarketingDataSet()

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={"success": True, "message": "가상 마케팅 데이터 생성 성공"}
        )

    except Exception as e:
        print(f"❌ Error in generateMarketingData(): {str(e)}")
        raise HTTPException(status_code=500, detail="서버 내부 오류 발생")

@marketingRouter.post("/marketing/create-virtual-data")
async def generateVirtualMarketingData(
    marketingService: MarketingServiceImpl = Depends(injectMarketingService)
):
    try:
        await marketingService.generateVirtualMarketingData()

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={"success": True, "message": "가상 마케팅 데이터 생성 성공"}
        )

    except Exception as e:
        print(f"❌ Error in generateMarketingData(): {str(e)}")
        raise HTTPException(status_code=500, detail="서버 내부 오류 발생")

@marketingRouter.post("/marketing/analysis-virtual-data")
async def requestAnalysis(
    marketingService: MarketingServiceImpl = Depends(injectMarketingService)
):
    try:
        result = await marketingService.requestAnalysis()

        return JSONResponse(status_code=202, content=result)

    except Exception as e:
        print(f"❌ Error in requestAnalysis(): {str(e)}")
        raise HTTPException(status_code=500, detail="서버 내부 오류 발생")

@marketingRouter.post("/marketing/virtual-data-list")
async def requestVirtualDataList(
    marketingService: MarketingServiceImpl = Depends(injectMarketingService)
):
    try:
        result = await marketingService.requestDataList()
        return JSONResponse(status_code=200, content=result)

    except Exception as e:
        print(f"❌ Error in requestVirtualDataList(): {str(e)}")
        raise HTTPException(status_code=500, detail="서버 내부 오류 발생")

@marketingRouter.post("/marketing/virtual-data-read")
async def requestVirtualDataRead(
    marketingService: MarketingServiceImpl = Depends(injectMarketingService)
):
    pass