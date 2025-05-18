import json
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter
from fastapi.responses import JSONResponse


webfluxThreadRouter = APIRouter()

@webfluxThreadRouter.get("/webflux/thread-test")
def webflux_thread_test_sync():
    thread_id_request = threading.get_ident()
    print(f"[1] Request handler - thread id: {thread_id_request}")

    def blocking_task():
        thread_id_executor = threading.get_ident()
        print(f"[2] Inside ThreadPoolExecutor - thread id: {thread_id_executor}")
        import time; time.sleep(1)
        return thread_id_executor

    # run task in executor
    with ThreadPoolExecutor(max_workers=10) as local_executor:
        future = local_executor.submit(blocking_task)
        thread_id_inside = future.result()

    thread_id_response = threading.get_ident()
    print(f"[3] Response handler - thread id: {thread_id_response}")

    response_data = {
        "request": thread_id_request,
        "inside_executor": thread_id_inside,
        "response": thread_id_response,
    }

    print(json.dumps(response_data, indent=2) + "\n", flush=True)
    return JSONResponse(status_code=200, content=response_data)
