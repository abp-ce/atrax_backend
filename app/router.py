from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.ext.asyncio import AsyncSession

from . import crud
from .dependencies import get_session
from .service import Parse
from .schemas import PhoneInfo
from .tasks import celery_parse, celery_parse_all_csv

router = APIRouter(prefix="/api", tags=["api"])


@router.get("/{phone_num}", response_model=PhoneInfo)
async def get_info(
    phone_num: str = Path(..., regex=r"^7[3489]\d{9}$"),
    session: AsyncSession = Depends(get_session),
):
    result = await crud.get_info(session, int(phone_num[1:11]))
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
    file_num: int = Path(..., ge=0, lt=len(Parse.REMOTE_URLS)),
):
    celery_parse.delay(Parse.REMOTE_URLS[file_num])
    return {"message": "Parse queued"}


@router.get("/parse_all/{is_fitered}", status_code=status.HTTP_202_ACCEPTED)
async def parse_all(is_filtered: bool = False):
    celery_parse_all_csv.delay(is_filtered)
    return {"message": "Parse all queued"}
