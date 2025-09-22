from __future__ import annotations
from fastapi import FastAPI


app = FastAPI(title="Arb Detector")


@app.get("/health")
def health():
return {"status": "ok"}
