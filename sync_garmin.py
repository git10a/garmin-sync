import json
import os
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2.service_account import Credentials

# 非公式クライアント。実行時に認証仕様差分で調整が必要になる可能性があります。
from garminconnect import Garmin

SPREADSHEET_ID = "1Dz8URjVnODUBY51xjL3iD6eEhv-K8Na7DPEQ638OMmY"
SHEET_NAME = "garmin_activity_raw"
TIMEZONE_OFFSET_HOURS = 9  # JST


def get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def build_gsheet_client() -> gspread.Client:
    raw = get_env("GOOGLE_SERVICE_ACCOUNT_JSON")
    info = json.loads(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def ensure_sheet(ws_client: gspread.Client):
    sh = ws_client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=30)
        ws.append_row([
            "date",
            "activity_id",
            "activity_name",
            "sport",
            "start_local",
            "distance_m",
            "moving_time_s",
            "elapsed_time_s",
            "avg_speed_mps",
            "avg_hr",
            "max_hr",
            "avg_cadence",
            "elev_gain_m",
            "calories",
            "device_name",
            "raw_json",
        ])
    return ws


def get_local_date_range(target_date: datetime):
    local_tz = timezone(timedelta(hours=TIMEZONE_OFFSET_HOURS))
    start_local = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=local_tz)
    end_local = start_local + timedelta(days=1)
    return start_local, end_local


def login_garmin() -> Garmin:
    email = get_env("GARMIN_EMAIL")
    password = get_env("GARMIN_PASSWORD")
    client = Garmin(email=email, password=password)
    client.login()
    return client


def fetch_activities_for_date(client: Garmin, target_date: datetime):
    # ライブラリ差異に備えて広めに取り、日付でフィルタする
    start_local, end_local = get_local_date_range(target_date)
    start_str = (start_local - timedelta(days=1)).strftime("%Y-%m-%d")
    end_str = (end_local + timedelta(days=1)).strftime("%Y-%m-%d")

    activities = client.get_activities_by_date(startdate=start_str, enddate=end_str)
    picked = []
    for a in activities:
        start_gmt = a.get("startTimeGMT") or a.get("startTimeLocal") or a.get("startTime")
        if not start_gmt:
            continue
        # Garminの文字列は末尾 .0 やタイムゾーン差異があるため雑に吸収
        parsed = None
        for candidate in [start_gmt.replace("Z", "+00:00"), start_gmt]:
            try:
                parsed = datetime.fromisoformat(candidate)
                break
            except Exception:
                pass
        if parsed is None:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        local_dt = parsed.astimezone(timezone(timedelta(hours=TIMEZONE_OFFSET_HOURS)))
        if start_local.date() == local_dt.date():
            picked.append(a)
    return picked


def get_detail(client: Garmin, activity_id: str | int):
    # メソッド名差異に備えて順に試す
    for method_name in ["get_activity_details", "get_activity", "get_activity_by_id"]:
        method = getattr(client, method_name, None)
        if callable(method):
            try:
                return method(activity_id)
            except Exception:
                pass
    return {}


def upsert_activity_rows(ws, activities, client: Garmin):
    existing = ws.get_all_values()
    id_to_row = {}
    for idx, row in enumerate(existing[1:], start=2):
        if len(row) >= 2 and row[1]:
            id_to_row[row[1]] = idx

    for a in activities:
        activity_id = str(a.get("activityId") or a.get("activity_id") or "")
        if not activity_id:
            continue
        detail = get_detail(client, activity_id)
        raw = detail if detail else a

        start_local = raw.get("startTimeLocal") or raw.get("startTimeGMT") or a.get("startTimeLocal") or a.get("startTimeGMT") or ""
        distance_m = raw.get("distance") or a.get("distance") or ""
        moving_time_s = raw.get("duration") or raw.get("movingDuration") or a.get("duration") or ""
        elapsed_time_s = raw.get("elapsedDuration") or a.get("elapsedDuration") or ""
        avg_speed_mps = raw.get("averageSpeed") or a.get("averageSpeed") or ""
        avg_hr = raw.get("averageHR") or a.get("averageHR") or ""
        max_hr = raw.get("maxHR") or a.get("maxHR") or ""
        avg_cadence = raw.get("averageRunCadence") or raw.get("averageBikeCadence") or ""
        elev_gain_m = raw.get("elevationGain") or a.get("elevationGain") or ""
        calories = raw.get("calories") or a.get("calories") or ""
        device_name = raw.get("deviceName") or raw.get("deviceId") or ""
        activity_name = raw.get("activityName") or a.get("activityName") or ""
        sport = raw.get("activityType", {}).get("typeKey") or a.get("activityType", {}).get("typeKey") or ""

        date_str = start_local[:10] if start_local else datetime.now(timezone(timedelta(hours=TIMEZONE_OFFSET_HOURS))).strftime("%Y-%m-%d")

        row_values = [
            date_str,
            activity_id,
            activity_name,
            sport,
            start_local,
            distance_m,
            moving_time_s,
            elapsed_time_s,
            avg_speed_mps,
            avg_hr,
            max_hr,
            avg_cadence,
            elev_gain_m,
            calories,
            device_name,
            json.dumps(raw, ensure_ascii=False),
        ]

        if activity_id in id_to_row:
            row_num = id_to_row[activity_id]
            ws.update(f"A{row_num}:P{row_num}", [row_values])
        else:
            ws.append_row(row_values)


def main():
    target_date_str = os.getenv("TARGET_DATE")
    if target_date_str:
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    else:
        target_date = datetime.now(timezone(timedelta(hours=TIMEZONE_OFFSET_HOURS)))

    gsheet_client = build_gsheet_client()
    ws = ensure_sheet(gsheet_client)

    garmin = login_garmin()
    activities = fetch_activities_for_date(garmin, target_date)
    upsert_activity_rows(ws, activities, garmin)

    print(f"Synced {len(activities)} Garmin activities for {target_date.strftime('%Y-%m-%d')}")


if __name__ == "__main__":
    main()
