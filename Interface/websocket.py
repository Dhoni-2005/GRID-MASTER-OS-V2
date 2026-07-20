"""
interface/websocket.py — Grid Master OS Phase 5
WebSocket transport placeholder for Phase 7 Distributed Runtime.
"""

def start_server(host: str = "0.0.0.0", port: int = 8765) -> None:
    raise NotImplementedError(
        "WebSocket server is not yet implemented. "
        "Target: Phase 7 — Distributed Grid Runtime."
    )

def broadcast(event: str, data: dict) -> None:
    raise NotImplementedError("WebSocket broadcast not yet implemented.")

def on_message(handler) -> None:
    raise NotImplementedError("WebSocket message handler not yet implemented.")
