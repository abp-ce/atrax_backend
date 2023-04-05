import asyncio

from celery import Celery, group
from celery.schedules import crontab

from .config import settings
from .database import engine
from .service import Parse

celery = Celery("tasks", broker=f"redis://{settings.redis_host}")


celery.conf.beat_schedule = {
    "add-every-day": {
        "task": "app.tasks.celery_parse_all_csv",
        "schedule": crontab(hour=15, minute=50),
    },
}


@celery.task
def celery_parse(file_num: int):
    asyncio.run(Parse(engine).parse_csv(file_num))


@celery.task
def celery_parse_all_csv():
    group(celery_parse.s(i) for i in range(len(Parse.REMOTE_URLS)))()
