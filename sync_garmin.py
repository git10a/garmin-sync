"""
Garmin Connect → Google Sheets 同期スクリプト

シート1: garmin_activity_raw   — アクティビティ詳細（ラップ・ランダイナミクス込み）
シート2: garmin_health_daily   — 日次健康データ（睡眠・HRV・Body Battery等）

認証優先順位:
  1. GARMIN_TOKENSTORE secret（JSON文字列）→ garth トークンで認証
  2. GARMIN_EMAIL + GARMIN_PASSWORD → メール/パスワードで認証
"""

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2.service_account import Credentials
from garminconnect import Garmin

SPREADSHEET_ID = "1Dz8URjVnODUBY51xjL3iD6eEhv-K8Na7DPEQ638OMmY"
SHEET_ACTIVITY = "garmin_activity_raw"
SHEET_HEALTH = "garmin_health_daily"
TIMEZONE_OFFSET_HOURS = 9  # JST

JST = timezone(timedelta(hours=TIMEZONE_OFFSET_HOURS))


# ── Google Sheets ──────────────────────────────────────────────────────────────

def build_gsheet_client() -> gspread.Client:
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")
    info = json.loads(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def ensure_sheet(sh, title: str, headers: list[str]):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=2000, cols=len(headers) + 5)
        ws.append_row(headers)
    return ws


ACTIVITY_HEADERS = [
    "date", "activity_id", "activity_name", "sport", "start_local",
    # 距離・時間
    "distance_m", "moving_time_s", "elapsed_time_s",
    # スピード・ペース
    "avg_speed_mps", "avg_pace_min_per_km",
    # 心拍
    "avg_hr", "max_hr",
    # ランニングダイナミクス
    "avg_cadence", "avg_stride_length_m",
    "avg_vertical_oscillation_cm", "avg_vertical_ratio_pct",
    "avg_ground_contact_time_ms", "avg_ground_contact_balance_pct",
    # パワー・効率
    "avg_power_w", "normalized_power_w",
    # 標高・消費
    "elev_gain_m", "elev_loss_m", "calories",
    # パフォーマンス
    "aerobic_training_effect", "anaerobic_training_effect",
    "training_stress_score", "vo2max_estimate",
    # ラップデータ（JSON）
    "laps_json",
    # デバイス
    "device_name",
    # 生データ
    "raw_json",
]

HEALTH_HEADERS = [
    "date",
    # 睡眠
    "sleep_start", "sleep_end", "sleep_total_seconds",
    "sleep_deep_s", "sleep_light_s", "sleep_rem_s", "sleep_awake_s",
    "sleep_score", "sleep_avg_spo2", "sleep_avg_hr", "sleep_avg_hrv",
    # HRV
    "hrv_weekly_avg", "hrv_last_night", "hrv_5min_high",
    # Body Battery
    "body_battery_high", "body_battery_low",
    # ストレス
    "avg_stress", "max_stress", "stress_qualifier",
    # 安静時心拍
    "resting_hr",
    # SpO2
    "avg_spo2", "min_spo2",
    # 歩数・活動量
    "total_steps", "step_goal", "floors_up", "floors_down",
    "active_calories", "total_calories",
    "moderate_intensity_min", "vigorous_intensity_min",
    # 生データ
    "raw_json",
]


# ── Garmin 認証 ────────────────────────────────────────────────────────────────

def login_garmin() -> Garmin:
    tokenstore_json = os.getenv("GARMIN_TOKENSTORE")
    if tokenstore_json:
        # トークンファイルを一時ディレクトリに展開して garth に読ませる
        token_data = json.loads(tokenstore_json)
        tmpdir = tempfile.mkdtemp()
        for fname, content in token_data.items():
            with open(os.path.join(tmpdir, fname), "w") as f:
                f.write(content if isinstance(content, str) else json.dumps(content))
        client = Garmin()
        client.garth.load(tmpdir)
        print("Garmin: トークン認証で接続")
        return client

    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")
    if not email or not password:
        raise RuntimeError("GARMIN_TOKENSTORE または GARMIN_EMAIL+GARMIN_PASSWORD が必要です")
    client = Garmin(email=email, password=password)
    client.login()
    print("Garmin: メール/パスワードで接続")
    return client


# ── ユーティリティ ─────────────────────────────────────────────────────────────

CELL_MAX = 49000  # Sheets の1セル上限は50,000文字


def _truncate(s: str) -> str:
    return s[:CELL_MAX] if len(s) > CELL_MAX else s


def safe_get(d, *keys):
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k)
        else:
            return ""
    return d if d is not None else ""


def parse_start_time(time_str: str):
    if not time_str:
        return None
    for candidate in [time_str.replace("Z", "+00:00"), time_str]:
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass
    return None


def date_str_jst(dt: datetime) -> str:
    return dt.astimezone(JST).strftime("%Y-%m-%d")


# ── アクティビティ同期 ─────────────────────────────────────────────────────────

def fetch_activities_for_date(client: Garmin, target_date: datetime) -> list:
    local_tz = JST
    start_local = datetime(target_date.year, target_date.month, target_date.day, tzinfo=local_tz)
    start_str = (start_local - timedelta(days=1)).strftime("%Y-%m-%d")
    end_str = (start_local + timedelta(days=1)).strftime("%Y-%m-%d")

    activities = client.get_activities_by_date(startdate=start_str, enddate=end_str)
    picked = []
    for a in activities:
        time_str = a.get("startTimeGMT") or a.get("startTimeLocal") or a.get("startTime") or ""
        dt = parse_start_time(time_str)
        if dt and dt.astimezone(JST).date() == start_local.date():
            picked.append(a)
    return picked


def get_activity_detail(client: Garmin, activity_id):
    for method_name in ["get_activity_details", "get_activity", "get_activity_by_id"]:
        m = getattr(client, method_name, None)
        if callable(m):
            try:
                return m(activity_id)
            except Exception:
                pass
    return {}


def get_laps(client: Garmin, activity_id):
    for method_name in ["get_activity_laps", "get_activity_splits"]:
        m = getattr(client, method_name, None)
        if callable(m):
            try:
                result = m(activity_id)
                if result:
                    return result
            except Exception:
                pass
    return []


def build_activity_row(a: dict, detail: dict, laps) -> list:
    raw = detail if detail else a

    start_local_str = (raw.get("startTimeLocal") or raw.get("startTimeGMT")
                       or a.get("startTimeLocal") or a.get("startTimeGMT") or "")
    date = start_local_str[:10] if start_local_str else ""

    distance_m = safe_get(raw, "distance") or safe_get(a, "distance")
    avg_speed = safe_get(raw, "averageSpeed") or safe_get(a, "averageSpeed")
    avg_pace = ""
    if avg_speed and float(avg_speed) > 0:
        avg_pace = round(1000 / 60 / float(avg_speed), 2)  # min/km

    sport = (safe_get(raw, "activityType", "typeKey")
             or safe_get(a, "activityType", "typeKey"))

    return [
        date,
        str(safe_get(a, "activityId") or safe_get(a, "activity_id")),
        safe_get(raw, "activityName") or safe_get(a, "activityName"),
        sport,
        start_local_str,
        # 距離・時間
        distance_m,
        safe_get(raw, "duration") or safe_get(raw, "movingDuration") or safe_get(a, "duration"),
        safe_get(raw, "elapsedDuration") or safe_get(a, "elapsedDuration"),
        # スピード・ペース
        avg_speed,
        avg_pace,
        # 心拍
        safe_get(raw, "averageHR") or safe_get(a, "averageHR"),
        safe_get(raw, "maxHR") or safe_get(a, "maxHR"),
        # ランニングダイナミクス
        safe_get(raw, "averageRunCadence") or safe_get(raw, "averageBikeCadence"),
        safe_get(raw, "avgStrideLength"),
        safe_get(raw, "avgVerticalOscillation"),
        safe_get(raw, "avgVerticalRatio"),
        safe_get(raw, "avgGroundContactTime"),
        safe_get(raw, "avgGroundContactBalance"),
        # パワー
        safe_get(raw, "avgPower"),
        safe_get(raw, "normPower"),
        # 標高・消費
        safe_get(raw, "elevationGain") or safe_get(a, "elevationGain"),
        safe_get(raw, "elevationLoss") or safe_get(a, "elevationLoss"),
        safe_get(raw, "calories") or safe_get(a, "calories"),
        # パフォーマンス指標
        safe_get(raw, "aerobicTrainingEffect"),
        safe_get(raw, "anaerobicTrainingEffect"),
        safe_get(raw, "trainingStressScore"),
        safe_get(raw, "vO2MaxValue") or safe_get(a, "vO2MaxValue"),
        # ラップ
        _truncate(json.dumps(laps, ensure_ascii=False) if laps else ""),
        # デバイス
        safe_get(raw, "deviceName") or safe_get(raw, "deviceId"),
        # 生データ
        _truncate(json.dumps(raw, ensure_ascii=False)),
    ]


def upsert_activities(ws, activities: list, client: Garmin):
    existing = ws.get_all_values()
    id_to_row = {row[1]: idx + 2 for idx, row in enumerate(existing[1:]) if len(row) >= 2 and row[1]}

    for a in activities:
        activity_id = str(safe_get(a, "activityId") or safe_get(a, "activity_id"))
        if not activity_id:
            continue
        detail = get_activity_detail(client, activity_id)
        laps = get_laps(client, activity_id)
        row = build_activity_row(a, detail, laps)

        col_end = chr(ord("A") + len(row) - 1)
        if activity_id in id_to_row:
            rn = id_to_row[activity_id]
            ws.update(f"A{rn}:{col_end}{rn}", [row])
        else:
            ws.append_row(row)


# ── 健康データ同期 ─────────────────────────────────────────────────────────────

def safe_call(client, method_name, *args, **kwargs):
    m = getattr(client, method_name, None)
    if callable(m):
        try:
            return m(*args, **kwargs)
        except Exception as e:
            print(f"  {method_name} 取得失敗: {e}")
    return None


def build_health_row(client: Garmin, date_str: str) -> list:
    # 睡眠
    sleep_raw = safe_call(client, "get_sleep_data", date_str) or {}
    sd = sleep_raw.get("dailySleepDTO") or sleep_raw
    sleep_start = sd.get("sleepStartTimestampLocal") or sd.get("sleepStartTimestampGMT") or ""
    sleep_end = sd.get("sleepEndTimestampLocal") or sd.get("sleepEndTimestampGMT") or ""
    if sleep_start and str(sleep_start).isdigit():
        sleep_start = datetime.fromtimestamp(int(sleep_start) / 1000, tz=JST).isoformat()
    if sleep_end and str(sleep_end).isdigit():
        sleep_end = datetime.fromtimestamp(int(sleep_end) / 1000, tz=JST).isoformat()

    # HRV
    hrv_raw = safe_call(client, "get_hrv_data", date_str) or {}
    hrv_summary = hrv_raw.get("hrvSummary") or hrv_raw

    # Body Battery
    bb_raw = safe_call(client, "get_body_battery", date_str, date_str) or []
    bb_high = bb_low = ""
    if isinstance(bb_raw, list) and bb_raw:
        vals = [v for item in bb_raw for v in (item.get("bodyBatteryValuesArray") or []) if v]
        nums = [v[1] for v in vals if isinstance(v, list) and len(v) >= 2]
        if nums:
            bb_high, bb_low = max(nums), min(nums)

    # ストレス
    stress_raw = safe_call(client, "get_stress_data", date_str) or {}
    avg_stress = stress_raw.get("avgStressLevel") or stress_raw.get("averageStressLevel") or ""
    max_stress = stress_raw.get("maxStressLevel") or ""
    stress_qual = stress_raw.get("stressQualifier") or ""

    # 安静時心拍
    rhr_raw = safe_call(client, "get_resting_heart_rate", date_str) or {}
    rhr = (rhr_raw.get("restingHeartRateValue")
           or rhr_raw.get("rhr")
           or safe_get(rhr_raw, "allMetrics", "metricsMap", "RESTING_HEART_RATE", 0, "value")
           or "")

    # SpO2
    spo2_raw = safe_call(client, "get_spo2_data", date_str) or {}
    avg_spo2 = spo2_raw.get("averageSpO2") or spo2_raw.get("avgSpo2") or ""
    min_spo2 = spo2_raw.get("lowestSpO2") or spo2_raw.get("minSpo2") or ""

    # 日次サマリー（歩数・消費）
    summary_raw = safe_call(client, "get_daily_summary", date_str) or {}
    # steps
    total_steps = (summary_raw.get("totalSteps")
                   or summary_raw.get("steps") or "")
    step_goal = summary_raw.get("dailyStepGoal") or summary_raw.get("stepGoal") or ""
    floors_up = summary_raw.get("floorsAscended") or ""
    floors_down = summary_raw.get("floorsDescended") or ""
    active_cal = summary_raw.get("activeKilocalories") or summary_raw.get("activeCalories") or ""
    total_cal = summary_raw.get("totalKilocalories") or summary_raw.get("totalCalories") or ""
    mod_min = summary_raw.get("moderateIntensityMinutes") or ""
    vig_min = summary_raw.get("vigorousIntensityMinutes") or ""

    # 全生データをまとめる
    raw_combined = {
        "sleep": sleep_raw,
        "hrv": hrv_raw,
        "body_battery": bb_raw,
        "stress": stress_raw,
        "resting_hr": rhr_raw,
        "spo2": spo2_raw,
        "daily_summary": summary_raw,
    }

    return [
        date_str,
        # 睡眠
        sleep_start, sleep_end,
        sd.get("sleepTimeSeconds") or sd.get("totalSleepSeconds") or "",
        sd.get("deepSleepSeconds") or "",
        sd.get("lightSleepSeconds") or "",
        sd.get("remSleepSeconds") or "",
        sd.get("awakeSleepSeconds") or "",
        sd.get("sleepScores", {}).get("overall", {}).get("value") if isinstance(sd.get("sleepScores"), dict) else sd.get("sleepScore") or "",
        sd.get("averageSpO2Value") or "",
        sd.get("averageHeartRate") or sd.get("avgSleepingHR") or "",
        sd.get("averageHRV") or sd.get("avgSleepingHRV") or "",
        # HRV
        hrv_summary.get("weeklyAvg") or "",
        hrv_summary.get("lastNight") or hrv_summary.get("lastNightAvg") or "",
        hrv_summary.get("lastNight5MinHigh") or "",
        # Body Battery
        bb_high, bb_low,
        # ストレス
        avg_stress, max_stress, stress_qual,
        # 安静時心拍
        rhr,
        # SpO2
        avg_spo2, min_spo2,
        # 歩数・活動量
        total_steps, step_goal, floors_up, floors_down,
        active_cal, total_cal, mod_min, vig_min,
        # 生データ
        _truncate(json.dumps(raw_combined, ensure_ascii=False)),
    ]


def upsert_health(ws, date_str: str, row: list):
    existing = ws.get_all_values()
    date_to_row = {r[0]: idx + 2 for idx, r in enumerate(existing[1:]) if r}

    col_end = chr(ord("A") + len(row) - 1)
    if date_str in date_to_row:
        rn = date_to_row[date_str]
        ws.update(f"A{rn}:{col_end}{rn}", [row])
    else:
        ws.append_row(row)


# ── メイン ────────────────────────────────────────────────────────────────────

def main():
    target_date_str = os.getenv("TARGET_DATE")
    if target_date_str:
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d").replace(tzinfo=JST)
    else:
        target_date = datetime.now(JST)

    date_str = target_date.strftime("%Y-%m-%d")
    print(f"対象日: {date_str}")

    gsheet_client = build_gsheet_client()
    sh = gsheet_client.open_by_key(SPREADSHEET_ID)
    ws_activity = ensure_sheet(sh, SHEET_ACTIVITY, ACTIVITY_HEADERS)
    ws_health = ensure_sheet(sh, SHEET_HEALTH, HEALTH_HEADERS)

    garmin = login_garmin()

    # アクティビティ
    print("アクティビティ取得中...")
    activities = fetch_activities_for_date(garmin, target_date)
    upsert_activities(ws_activity, activities, garmin)
    print(f"  {len(activities)} 件書き込み完了")

    # 健康データ
    print("健康データ取得中...")
    health_row = build_health_row(garmin, date_str)
    upsert_health(ws_health, date_str, health_row)
    print("  健康データ書き込み完了")

    print(f"完了: {date_str}")


if __name__ == "__main__":
    main()
