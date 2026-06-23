import os
import uvicorn
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from news_collector import main as run_collector

app = FastAPI(
    title="News Collector API",
    description="FastAPI wrapper to trigger news collection via HTTP request for Koyeb deployment",
    version="1.0.0"
)

# Простой статус-проверка для Koyeb health checks
@app.get("/")
def read_root():
    return {
        "status": "ok",
        "message": "News Collector API is active and waiting for triggers",
        "database_mode": "PostgreSQL" if os.environ.get("DATABASE_URL") else "SQLite (local)"
    }

# Эндпоинт для запуска сбора новостей
@app.get("/collect")
def collect_news(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_collector)
    return JSONResponse(
        content={
            "status": "triggered",
            "message": "News collection started in the background"
        },
        status_code=202
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
