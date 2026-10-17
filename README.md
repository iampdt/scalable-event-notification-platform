# Scalable Event Notification Platform

## Local development

```sh
docker compose up -d
python -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
.venv/bin/alembic upgrade head
.venv/bin/python -m app.scripts.seed
.venv/bin/uvicorn app.main:app --reload
```

In another terminal, run `.venv/bin/python -m app.workers.delivery_worker`.

The seed command prints the producer key only when it creates it. Use that value
as `Authorization: Bearer <key>` when calling `POST /v1/events`.
