from fastapi import APIRouter, Depends, HTTPException, status, Request, Body
from fastapi.responses import JSONResponse

from aiomysql import Pool

from async_db.database import getMySqlPool
from marketing.controller.request_form.read_request_form import ReadRequestForm
from marketing.controller.request_form.remove_request_form import RemoveRequestForm
from marketing.controller.request_form.update_request_form import UpdateRequestForm
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
    requestForm: ReadRequestForm = Body(...),
    marketingService: MarketingServiceImpl = Depends(injectMarketingService)
):
    try:
        customer_id = requestForm.customer_id
        result = await marketingService.readVirtualMarketingData(customer_id)
        return JSONResponse(status_code=200, content={"status": "success", "data": result})

    except Exception as e:
        print(f"❌ Error in requestVirtualDataRead(): {str(e)}")
        raise HTTPException(status_code=500, detail="서버 내부 오류 발생")

@marketingRouter.post("/marketing/virtual-data-update")
async def requestVirtualDataUpdate(
    requestForm: UpdateRequestForm = Body(...),
    marketingService: MarketingServiceImpl = Depends(injectMarketingService)
):
    try:
        result = await marketingService.updateVirtualMarketingData(requestForm)
        return JSONResponse(status_code=200, content={"status": "success", "updated": result})

    except Exception as e:
        print(f"❌ Error in requestVirtualDataUpdate(): {str(e)}")
        raise HTTPException(status_code=500, detail="서버 내부 오류 발생")

@marketingRouter.post("/marketing/virtual-data-remove")
async def requestVirtualDataRemove(
    requestForm: RemoveRequestForm = Body(...),
    marketingService: MarketingServiceImpl = Depends(injectMarketingService)
):
    try:
        deleted = await marketingService.removeVirtualMarketingData(requestForm.customer_id)
        return JSONResponse(status_code=200, content={"status": "success", "deleted": deleted})
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"❌ Error in requestVirtualDataRemove(): {str(e)}")
        raise HTTPException(status_code=500, detail="서버 내부 오류 발생")
