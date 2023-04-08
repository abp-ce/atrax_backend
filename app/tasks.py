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
        "schedule": crontab(hour=7, minute=32),
        "args": (False,),
    },
}


@celery.task
def celery_parse(file_name: str):
    asyncio.run(Parse(engine).parse_csv(file_name))


@celery.task
def celery_parse_all_csv(is_filtered: bool = False):
    files_to_parse = (
        Parse.filter_remote_urls() if is_filtered else Parse.REMOTE_URLS
    )
    group(celery_parse.s(file_name) for file_name in files_to_parse)()
