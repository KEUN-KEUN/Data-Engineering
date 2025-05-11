from fastapi import APIRouter, WebSocket, WebSocketDisconnect

websocketController = APIRouter()

@websocketController.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    websocket.app.state.connections.add(websocket)

    try:
        while True:
            await websocket.receive_text()  # 텍스트는 무시
    except WebSocketDisconnect:
        websocket.app.state.connections.remove(websocket)
