from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import text

from .config import settings
from .db import create_job, get_engine


def maybe_create_daily_job() -> None:
    if not settings.auto_daily_enabled:
        return

    now_local = datetime.now(ZoneInfo(settings.timezone))
    if now_local.hour < settings.daily_hour:
        return

    today = now_local.date().isoformat()

    with get_engine().begin() as conn:
        existing = conn.execute(
            text(
                """
                SELECT id FROM jobs
                WHERE job_type='daily_update'
                  AND requested_by='scheduler'
                  AND substr(requested_at, 1, 10)=:today
                LIMIT 1
                """
            ),
            {"today": today},
        ).first()
        if existing:
            return

        agent = conn.execute(
            text("SELECT * FROM agent_status WHERE status='online' ORDER BY updated_at DESC LIMIT 1")
        ).first()
        if not agent or not agent._mapping.get("online_since"):
            return

        online_since = datetime.fromisoformat(str(agent._mapping["online_since"]).replace("Z", "+00:00"))
        min_uptime = timedelta(minutes=settings.online_minutes)
        if datetime.utcnow() - online_since.replace(tzinfo=None) < min_uptime:
            return

    create_job(
        job_type="daily_update",
        requested_by="scheduler",
        payload={"run_mode": "all", "source": "bisp"},
    )
