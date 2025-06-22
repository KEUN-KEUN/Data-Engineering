from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from aysnc_lab.async_context import get_user_status
from aysnc_lab.service.async_lab_service_impl import AsyncLabServiceImpl

asyncLabRouter = APIRouter()
service = AsyncLabServiceImpl()

@asyncLabRouter.post("/async-lab/request-process")
async def request_async_process(request: Request):
    body = await request.json()
    user_token = body.get("user_token")

    if not user_token:
        return JSONResponse(status_code=400, content={"error": "Missing user_token"})

    result = await service.enqueue_user_request(user_token)
    return JSONResponse(content=result)


@asyncLabRouter.get("/async-lab/status")
async def check_status(user_token: str):
    status = await get_user_status(user_token)
    return JSONResponse(content={"user_token": user_token, "status": status})