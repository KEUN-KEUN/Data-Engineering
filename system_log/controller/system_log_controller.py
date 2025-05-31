from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.responses import JSONResponse

from aiomysql import Pool

from async_db.database import getMySqlPool
from system_log.service.system_log_service_impl import SystemLogServiceImpl

systemLogRouter = APIRouter()

async def injectSystemLogService(db_pool: Pool = Depends(getMySqlPool)) -> SystemLogServiceImpl:
    return SystemLogServiceImpl(db_pool)

@systemLogRouter.post("/system-log/record-csv")
async def recordSystemLogCsv(
    systemLogService: SystemLogServiceImpl = Depends(injectSystemLogService)
):
    try:
        await systemLogService.recordCsv()

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={"success": True, "message": "시스템 로그 기록 성공"}
        )

    except Exception as e:
        print(f"❌ Error in recordSystemLogCsv(): {str(e)}")
        raise HTTPException(status_code=500, detail="서버 내부 오류 발생")

@systemLogRouter.post("/system-log/analysis")
async def analysisSystemLogCsv(
    systemLogService: SystemLogServiceImpl = Depends(injectSystemLogService)
):
    try:
        analysisResult = await systemLogService.analysisCsv()

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={
                "success": True,
                "message": "시스템 로그 분석 성공",
                "data": analysisResult
            }
        )

    except Exception as e:
        print(f"❌ Error in analysisSystemLogCsv(): {str(e)}")
        raise HTTPException(status_code=500, detail="서버 내부 오류 발생")