from fastapi import APIRouter
from fastapi.responses import JSONResponse
from concurrent.futures import ThreadPoolExecutor
import threading
import asyncio
import json

webfluxThreadRouter = APIRouter()
executor = ThreadPoolExecutor(max_workers=10)


def step1_request_logic():
    before = threading.get_ident()
    return before


def step2_blocking_task(before_id):
    inside = threading.get_ident()
    return {"before": before_id, "inside": inside}


def step3_finalize_response(data):
    data["after"] = threading.get_ident()
    return data


@webfluxThreadRouter.get("/webflux/thread-test")
async def webflux_thread_test():
    before = await asyncio.get_event_loop().run_in_executor(executor, step1_request_logic)

    inside_data = await asyncio.get_event_loop().run_in_executor(executor, step2_blocking_task, before)

    final_data = await asyncio.get_event_loop().run_in_executor(executor, step3_finalize_response, inside_data)

    print("==== START ====")
    print(json.dumps(final_data, indent=2))
    print("==== END ====")

    return JSONResponse(content=final_data)
