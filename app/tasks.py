import asyncio

from celery import Celery
from celery.schedules import crontab

from .config import settings
from .service import Parse

celery = Celery("tasks", broker=f"redis://{settings.redis_host}")


celery.conf.beat_schedule = {
    "add-every-day": {
        "task": "app.tasks.celery_parse_all_csv",
        "schedule": crontab(hour=4, minute=20),
    },
}


@celery.task
def celery_parse(file_num: int):
    asyncio.run(Parse().parse_csv(file_num))


@celery.task
def celery_parse_all_csv():
    asyncio.run(Parse().parse_all_csv())
