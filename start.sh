#!/bin/sh
sleep 5
alembic upgrade head
uvicorn main:app --host 0.0.0.0 --reload
