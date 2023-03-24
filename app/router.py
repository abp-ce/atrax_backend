from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from . import crud
from .dependencies import get_session
from .schemas import PhoneInfo
from .tasks import celery_parse, celery_parse_all_csv

router = APIRouter(prefix="/api", tags=["api"])


@router.get("/{phone_num}", response_model=PhoneInfo)
async def get_info(
    phone_num: str, session: AsyncSession = Depends(get_session)
):
    try:
        result = await crud.get_info(
            session, int(phone_num[1:4]), int(phone_num[4:11])
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="К сожалению, не смог прочитать номер. Проверьте его.",
        )
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="К сожалению, номер не найден.",
        )
    return PhoneInfo(
        inn=result[0],
        operator=result[1],
        region=result[2],
        sub_region=result[3],
    )


@router.get("/parse/{file_num}", status_code=status.HTTP_202_ACCEPTED)
async def parse(
    file_num: int,
):
    celery_parse.delay(file_num)
    return {"message": "Parse queued"}


@router.get("/parse_all/", status_code=status.HTTP_202_ACCEPTED)
async def parse_all():
    celery_parse_all_csv.delay()
    return {"message": "Parse all queued"}
