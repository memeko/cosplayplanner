#!/usr/bin/env python3
"""End-to-end QA smoke test for the photocosplay module.

This script runs an isolated scenario against FastAPI TestClient:
1. user registration/login/logout,
2. contest request creation,
3. admin approval,
4. participant work submission,
5. open/closed judging vote checks,
6. finished contest results checks.

By default the script uses /tmp/photocosplay_qa.sqlite3 and removes it after run.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any

from PIL import Image


def make_jpeg_bytes(color: tuple[int, int, int]) -> bytes:
    image = Image.new("RGB", (640, 960), color)
    buffer = BytesIO()
    image.save(buffer, format="JPEG", quality=92)
    return buffer.getvalue()


def assert_ok(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def post_form(client: Any, url: str, data: dict[str, Any] | None = None, files: Any = None) -> Any:
    response = client.post(url, data=data or {}, files=files)
    assert_ok(response.status_code in {200, 303}, f"POST {url} failed with status {response.status_code}")
    return response


def fmt(day: date) -> str:
    return day.isoformat()


def configure_env(db_path: Path, secret_key: str) -> None:
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"
    os.environ["SECRET_KEY"] = secret_key

    # Disable background integrations for deterministic local QA.
    os.environ["TELEGRAM_BOT_ENABLED"] = "0"
    os.environ["VK_BOT_ENABLED"] = "0"
    os.environ["VK_IMPORT_ENABLED"] = "0"

    # Keep loops asleep if something still starts.
    os.environ["CONTENT_TELEGRAM_LOOP_SLEEP"] = "3600"
    os.environ["TELEGRAM_LOOP_SLEEP"] = "3600"
    os.environ["VK_BOT_LOOP_SLEEP"] = "3600"
    os.environ["EXTERNAL_IMPORT_LOOP_SLEEP"] = "3600"


def run_scenario(db_path: Path) -> None:
    configure_env(db_path, secret_key="photocosplay-qa-secret")

    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    # Import only after env setup so app picks isolated DB.
    from fastapi.testclient import TestClient

    from app.main import (
        PhotoContest,
        PhotoContestEntry,
        PhotoContestEntryPhoto,
        PhotoContestRequest,
        PhotoContestVote,
        SessionLocal,
        User,
        app,
        password_context,
    )

    today = date.today()
    submission_start_open = today - timedelta(days=1)
    submission_end_open = today + timedelta(days=2)
    results_future = today + timedelta(days=10)
    submission_end_judging = today - timedelta(days=1)
    results_still_future = today + timedelta(days=5)
    results_finished = today - timedelta(days=1)

    with TestClient(app) as client:
        def register(username: str, email: str, password: str = "pass1234") -> None:
            # Registration requires a real email verification delivery. Seed
            # isolated QA users directly and keep authentication itself covered
            # through the public login route below.
            with SessionLocal() as db:
                db.add(
                    User(
                        username=username,
                        email=email,
                        password_hash=password_context.hash(password),
                    )
                )
                db.commit()

        def login(login_value: str, password: str = "pass1234") -> None:
            post_form(client, "/login", data={"login": login_value, "password": password})

        def logout() -> None:
            post_form(client, "/logout")

        # 1) Users
        register("brfox_cosplay", "admin@example.com")
        logout()
        register("contest_creator", "creator@example.com")
        logout()
        register("contest_participant", "participant@example.com")
        logout()
        register("contest_viewer", "viewer@example.com")
        logout()

        # 2) Creator submits request
        login("contest_creator")
        post_form(
            client,
            "/photocosplay/requests/new",
            data={
                "title": "QA Фотокосплей 2026",
                "submission_start_date": fmt(submission_start_open),
                "submission_end_date": fmt(submission_end_open),
                "results_date": fmt(results_future),
                "nomination_title": ["Одиночный фотокосплей", "Групповой фотокосплей"],
                "nomination_places": ["3", "2"],
                "festival_id": "",
                "judging_format": "open",
                "judges_input": "",
                "rules_markdown": "Тестовые **правила** конкурса.",
                "prizes_markdown": "Тестовые призы.",
                "max_entries_per_participant": "2",
                "max_photos_per_participant": "2",
                "participant_visibility": "all",
            },
        )

        with SessionLocal() as db:
            contest_request = db.query(PhotoContestRequest).order_by(PhotoContestRequest.id.desc()).first()
            assert_ok(contest_request is not None, "Contest request was not created")
            request_id = int(contest_request.id)

        logout()

        # 3) Admin approves request
        login("brfox_cosplay")
        post_form(client, f"/photocosplay/requests/{request_id}/approve")

        with SessionLocal() as db:
            approved_request = db.get(PhotoContestRequest, request_id)
            assert_ok(approved_request is not None, "Approved request missing")
            assert_ok(approved_request.status == "approved", "Request status is not approved")
            contest = db.query(PhotoContest).order_by(PhotoContest.id.desc()).first()
            assert_ok(contest is not None, "Contest was not created on approval")
            contest_id = int(contest.id)

        logout()

        # 4) Participant submits work during open submission phase.
        login("contest_participant")
        files = [
            ("photos", ("work1.jpg", make_jpeg_bytes((200, 80, 120)), "image/jpeg")),
            ("photos", ("work2.jpg", make_jpeg_bytes((60, 140, 210)), "image/jpeg")),
        ]
        post_form(
            client,
            f"/photocosplay/{contest_id}/submit",
            data={
                "nomination_title": "Одиночный фотокосплей",
                "fandom": "Nier",
                "entry_person": ["contest_participant", "friend_photo"],
                "entry_role": ["cosplayer", "photographer"],
                "entry_character": ["2B", "A2"],
                "agree_rules": "1",
            },
            files=files,
        )

        with SessionLocal() as db:
            entry = (
                db.query(PhotoContestEntry)
                .filter(PhotoContestEntry.contest_id == contest_id)
                .filter(PhotoContestEntry.participant_user_id.isnot(None))
                .order_by(PhotoContestEntry.id.desc())
                .first()
            )
            assert_ok(entry is not None, "Contest entry was not created")
            photos = (
                db.query(PhotoContestEntryPhoto)
                .filter(PhotoContestEntryPhoto.contest_id == contest_id)
                .order_by(PhotoContestEntryPhoto.id.asc())
                .all()
            )
            assert_ok(len(photos) == 2, f"Expected 2 photos, got {len(photos)}")
            voted_photo_id = int(photos[0].id)

        response = client.get("/photocosplay?participating_only=1")
        assert_ok("QA Фотокосплей 2026" in response.text, "Participating filter did not show contest")
        response = client.get(f"/photocosplay/{contest_id}")
        assert_ok("Мои заявки" in response.text, "Participant cannot see own entry before judging")
        assert_ok("Редактировать" in response.text, "Participant cannot edit own entry before judging")
        post_form(
            client,
            f"/photocosplay/{contest_id}/submit",
            data={
                "nomination_title": "Групповой фотокосплей",
                "fandom": "Second allowed entry",
                "entry_person": ["contest_participant"],
                "entry_role": ["cosplayer"],
                "entry_character": ["A2"],
                "agree_rules": "1",
            },
            files=[("photos", ("second.jpg", make_jpeg_bytes((110, 90, 180)), "image/jpeg"))],
        )
        post_form(
            client,
            f"/photocosplay/{contest_id}/submit",
            data={
                "nomination_title": "Групповой фотокосплей",
                "fandom": "Rejected third entry",
                "entry_person": ["contest_participant"],
                "entry_role": ["cosplayer"],
                "entry_character": ["9S"],
                "agree_rules": "1",
            },
            files=[("photos", ("third.jpg", make_jpeg_bytes((80, 80, 80)), "image/jpeg"))],
        )
        with SessionLocal() as db:
            participant_user = db.query(User).filter(User.username == "contest_participant").one()
            participant_entries = db.query(PhotoContestEntry).filter(
                PhotoContestEntry.contest_id == contest_id,
                PhotoContestEntry.participant_user_id == participant_user.id,
            ).all()
            assert_ok(len(participant_entries) == 2, "Per-participant entry limit was not enforced")
        logout()

        # The creator can premoderate submissions before judging starts, while a
        # regular viewer cannot see them yet.
        login("contest_creator")
        response = client.get(f"/photocosplay/{contest_id}")
        assert_ok("Премодерация заявок" in response.text, "Creator premoderation block is missing")
        assert_ok("@contest_participant" in response.text, "Creator cannot see the submitted entry")
        assert_ok("максимальный размер каждого файла — 20 МБ" in response.text, "Per-file limit is missing")
        assert_ok("Участник:" not in response.text, "Participant label is still shown on preview cards")
        post_form(
            client,
            f"/photocosplay/{contest_id}/submit",
            data={
                "entry_id": str(entry.id),
                "nomination_title": "Одиночный фотокосплей",
                "fandom": "NieR:Automata",
                "entry_person": ["contest_participant", "friend_photo"],
                "entry_role": ["cosplayer", "photographer"],
                "entry_character": ["2B"],
                "agree_rules": "1",
            },
        )
        with SessionLocal() as db:
            edited_entry = db.get(PhotoContestEntry, entry.id)
            assert_ok(edited_entry is not None and edited_entry.fandom == "NieR:Automata", "Creator edit failed")
            remaining_photos = db.query(PhotoContestEntryPhoto).filter(PhotoContestEntryPhoto.entry_id == entry.id).all()
            assert_ok(len(remaining_photos) == 2, "Metadata edit unexpectedly replaced photos")
        logout()

        login("contest_viewer")
        response = client.get(f"/photocosplay/{contest_id}")
        assert_ok("@contest_participant" not in response.text, "Viewer can see entries before judging")
        post_form(
            client,
            f"/photocosplay/{contest_id}/submit",
            data={
                "nomination_title": "Групповой фотокосплей",
                "fandom": "Temporary entry",
                "entry_person": ["contest_viewer"],
                "entry_role": ["cosplayer"],
                "entry_character": ["Tester"],
                "agree_rules": "1",
            },
            files=[("photos", ("viewer.jpg", make_jpeg_bytes((100, 180, 120)), "image/jpeg"))],
        )
        response = client.get(f"/photocosplay/{contest_id}")
        assert_ok("Temporary entry" in response.text, "Participant cannot see own new entry")
        assert_ok("NieR:Automata" not in response.text, "Participant can see another entry before judging")
        with SessionLocal() as db:
            viewer_user = db.query(User).filter(User.username == "contest_viewer").one()
            viewer_entry = db.query(PhotoContestEntry).filter(
                PhotoContestEntry.contest_id == contest_id,
                PhotoContestEntry.participant_user_id == viewer_user.id,
            ).one()
            viewer_entry_id = int(viewer_entry.id)
        logout()

        login("contest_creator")
        post_form(client, f"/photocosplay/{contest_id}/entries/{viewer_entry_id}/delete")
        with SessionLocal() as db:
            assert_ok(db.get(PhotoContestEntry, viewer_entry_id) is None, "Creator delete failed")

        # 5) Move contest to judging phase.
        post_form(
            client,
            f"/photocosplay/{contest_id}/edit",
            data={
                "title": "QA Фотокосплей 2026",
                "submission_start_date": fmt(submission_start_open),
                "submission_end_date": fmt(submission_end_judging),
                "results_date": fmt(results_still_future),
                "nomination_title": ["Одиночный фотокосплей", "Групповой фотокосплей"],
                "nomination_places": ["3", "2"],
                "festival_id": "",
                "judging_format": "open",
                "judges_input": "",
                "rules_markdown": "Тестовые **правила** конкурса.",
                "prizes_markdown": "Тестовые призы.",
                "max_entries_per_participant": "2",
                "max_photos_per_participant": "2",
                "participant_visibility": "all",
            },
        )
        response = client.get("/photocosplay?status=judging")
        assert_ok("QA Фотокосплей 2026" in response.text, "Judging filter did not show contest")
        logout()

        # 6) Viewer votes in open judging.
        login("contest_viewer")
        post_form(client, f"/photocosplay/{contest_id}/votes", data={"photo_vote": [str(voted_photo_id)]})

        with SessionLocal() as db:
            votes = db.query(PhotoContestVote).filter(PhotoContestVote.contest_id == contest_id).all()
            assert_ok(len(votes) == 1, f"Expected 1 vote, got {len(votes)}")
        logout()

        # 7) Closed judging restriction.
        login("contest_creator")
        post_form(
            client,
            f"/photocosplay/{contest_id}/edit",
            data={
                "title": "QA Фотокосплей 2026",
                "submission_start_date": fmt(submission_start_open),
                "submission_end_date": fmt(submission_end_judging),
                "results_date": fmt(results_still_future),
                "nomination_title": ["Одиночный фотокосплей", "Групповой фотокосплей"],
                "nomination_places": ["3", "2"],
                "festival_id": "",
                "judging_format": "closed",
                "judges_input": "brfox_cosplay",
                "rules_markdown": "Тестовые **правила** конкурса.",
                "prizes_markdown": "Тестовые призы.",
                "max_entries_per_participant": "2",
                "max_photos_per_participant": "2",
                "participant_visibility": "all",
            },
        )
        logout()

        login("contest_viewer")
        post_form(client, f"/photocosplay/{contest_id}/votes", data={"photo_vote": [str(voted_photo_id)]})
        with SessionLocal() as db:
            votes = db.query(PhotoContestVote).filter(PhotoContestVote.contest_id == contest_id).all()
            assert_ok(len(votes) == 1, "Viewer should not be able to vote in closed judging")
        logout()

        # 8) Finish contest and check results visibility.
        login("contest_creator")
        post_form(
            client,
            f"/photocosplay/{contest_id}/edit",
            data={
                "title": "QA Фотокосплей 2026",
                "submission_start_date": fmt(submission_start_open),
                "submission_end_date": fmt(submission_end_judging),
                "results_date": fmt(results_finished),
                "nomination_title": ["Одиночный фотокосплей", "Групповой фотокосплей"],
                "nomination_places": ["3", "2"],
                "festival_id": "",
                "judging_format": "open",
                "judges_input": "",
                "rules_markdown": "Тестовые **правила** конкурса.",
                "prizes_markdown": "Тестовые призы.",
                "max_entries_per_participant": "2",
                "max_photos_per_participant": "2",
                "participant_visibility": "winners",
            },
        )

        response = client.get(f"/photocosplay/{contest_id}")
        assert_ok("Результаты" in response.text, "Results block is missing on finished contest page")
        assert_ok("@contest_participant" in response.text, "Winner alias is missing in results")

        response = client.get("/photocosplay?status=finished")
        assert_ok("QA Фотокосплей 2026" in response.text, "Finished filter did not show contest")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run photocosplay E2E QA smoke scenario")
    parser.add_argument(
        "--db-path",
        default="/tmp/photocosplay_qa.sqlite3",
        help="Path to isolated SQLite database (default: /tmp/photocosplay_qa.sqlite3)",
    )
    parser.add_argument(
        "--keep-db",
        action="store_true",
        help="Do not remove isolated database after run",
    )
    parser.add_argument(
        "--keep-media",
        action="store_true",
        help="Do not remove newly generated media files after run",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    media_dir = repo_root / "app" / "static" / "media"
    media_dir.mkdir(parents=True, exist_ok=True)
    media_before = {item.resolve() for item in media_dir.iterdir() if item.is_file()}

    db_path = Path(args.db_path).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    try:
        run_scenario(db_path)
        print("QA scenario completed successfully")
        return 0
    except AssertionError as exc:
        print(f"QA scenario failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"QA scenario crashed: {exc}", file=sys.stderr)
        return 1
    finally:
        if not args.keep_media:
            for item in media_dir.iterdir():
                if not item.is_file():
                    continue
                if item.resolve() in media_before:
                    continue
                try:
                    item.unlink()
                except OSError:
                    pass
        if not args.keep_db and db_path.exists():
            try:
                db_path.unlink()
            except OSError:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
