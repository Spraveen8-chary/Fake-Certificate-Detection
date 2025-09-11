from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from src.core.config import settings   # import your config

# -------------------------
# Initialize FastAPI App
# -------------------------
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    debug=settings.debug
)

app.mount("/static", StaticFiles(directory="static"), name = "static")

templates = Jinja2Templates(directory="templates")


@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "environment": settings.environment,
        "debug": settings.debug,
        "log_level": settings.log_level
    }


@app.get("/")
def root():
    return {
        "message": f"Welcome to {settings.app_name} API 🚀",
        "version": settings.app_version,
        "docs": "/docs"
    }


@app.get("/config")
def get_config():
    return {
        "app": {
            "name": settings.app_name,
            "version": settings.app_version,
            "environment": settings.environment,
            "debug": settings.debug,
        },
        "storage": {
            "data_dir": str(settings.storage.data_dir),
            "output_dir": str(settings.storage.output_dir),
            "cache_dir": str(settings.storage.cache_dir),
            "temp_dir": str(settings.storage.temp_dir),
            "log_file": str(settings.storage.log_file),
            "database_url": settings.storage.database_url,
        },
        "security": {
            "secret_key": settings.security.secret_key[:6] + "*****",  # hide real key
            "encrypt_credentials": settings.security.encrypt_credentials,
            "rate_limiting": settings.security.enable_rate_limiting,
            "audit_logging": settings.security.audit_logging,
        },
        "monitoring": {
            "metrics_enabled": settings.monitoring.enable_metrics,
            "metrics_port": settings.monitoring.metrics_port,
            "health_check_enabled": settings.monitoring.enable_health_check,
        }
    }


@app.get("/index", response_class=HTMLResponse)
async def index(request : Request):
    return templates.TemplateResponse("index.html", {"request": request, "title": "Home Page"})
