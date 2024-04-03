"""
Copyright (c) 2024 - Bizware International
"""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import logging
from datetime import datetime, timedelta

from routers.auth.auth_routes import router as auth_router
# from com.bizware.routers.product_routes import router as product_router
# from com.bizware.routers.user_routes import router as user_router
from routers.sales_routes import router as sales_router
# from com.bizware.routers.ui_routes import router as ui_router

from utils.SchedulerJob import scheduler

# scheduler = BackgroundScheduler()
# job = scheduler.add_job(print_mongo, 'interval', seconds=2)

logging.basicConfig(
    format='%(asctime)s - %(process)s - %(name)s:%(lineno)d - %(levelname)s -'
           ' %(message)s',
    level=logging.INFO,
)
b = logging.getLogger(__name__)

app = FastAPI(debug=True, reload=True)

scheduler.start()

app.mount("/static", StaticFiles(directory="src/static"), name="static")

origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins
)

app.include_router(auth_router)
# app.include_router(user_router)
# app.include_router(product_router)
app.include_router(sales_router)
# app.include_router(ui_router)
