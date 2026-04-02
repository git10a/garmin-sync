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
import time
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2.service_account import Credentials
from garminconnect import Garmin, GarminConnectTooManyRequestsError

SPREADSHEET_ID = "1Dz8URjVnODUBY51xjL3iD6eEhv-K8Na7DPEQ638OMmY"
SHEET_ACTIVITY = "garmin_activity_raw"
SHEET_HEALTH = "garmin_health_daily"
TIMEZONE_OFFSET_HOURS = 9  # JST

JST = timezone(timedelta(hours=TIMEZONE_OFFSET_HOURS))

CELL_MAX = 49000  # Sheets の1セル上限は50,000文字
RETRY_MAX = 3
RETRY_BASE_WAIT = 30  # 秒


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


ACTIVITY_HEADERS = [
    "date", "activity_id", "activity_name", "sport", "start_local",
    "distance_m", "moving_time_s", "elapsed_time_s",
    "avg_speed_mps", "avg_pace_min_per_km",
    "avg_hr", "max_hr",
    "avg_cadence_spm", "avg_stride_length_cm",
    "avg_vertical_oscillation_cm", "avg_vertical_ratio_pct",
    "avg_ground_contact_time_ms",
    "avg_power_w", "normalized_power_w",
    "elev_gain_m", "elev_loss_m", "calories",
    "aerobic_te", "anaerobic_te", "vo2max_estimate",
    "training_load", "diff_body_battery",
    "hr_zone1_s", "hr_zone2_s", "hr_zone3_s", "hr_zone4_s", "hr_zone5_s",
    "fastest_1k_s", "fastest_5k_s",
    "steps", "laps_json", "raw_json",
]

HEALTH_HEADERS = [
    "date",
    "sleep_start", "sleep_end", "sleep_total_s",
    "sleep_deep_s", "sleep_light_s", "sleep_rem_s", "sleep_awake_s",
    "sleep_score", "sleep_avg_hr", "sleep_avg_respiration",
    "sleep_awake_count", "sleep_avg_stress",
    "hrv_weekly_avg", "hrv_last_night", "hrv_5min_high",
    "body_battery_high", "body_battery_low",
    "avg_stress", "max_stress",
    "resting_hr",
    "avg_spo2", "min_spo2",
    "total_steps", "active_calories", "moderate_intensity_min", "vigorous_intensity_min",
    "raw_json",
]


def ensure_sheet_with_headers(sh, title: str, headers: list[str]):
    """シートを取得または作成し、ヘッダー行を常に最新に保つ。"""
    try:
        ws = sh.worksheet(title)
        # ヘッダー行を更新
        ws.update("1:1", [headers])
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=2000, cols=len(headers) + 5)
        ws.append_row(headers)
    return ws


# ── Garmin 認証 ────────────────────────────────────────────────────────────────

def garmin_retry(func, *args, **kwargs):
    """429 レートリミット時に exponential backoff でリトライする。"""
    for attempt in range(RETRY_MAX):
        try:
            return func(*args, **kwargs)
        except (GarminConnectTooManyRequestsError, Exception) as e:
            is_rate_limit = isinstance(e, GarminConnectTooManyRequestsError) or "429" in str(e)
            if not is_rate_limit or attempt == RETRY_MAX - 1:
                raise
            wait = RETRY_BASE_WAIT * (2 ** attempt)
            print(f"  429 レートリミット。{wait}秒待機してリトライ ({attempt + 1}/{RETRY_MAX})...")
            time.sleep(wait)


def login_garmin() -> Garmin:
    tokenstore_json = os.getenv("GARMIN_TOKENSTORE")
    if tokenstore_json:
        token_data = json.loads(tokenstore_json)
        tmpdir = tempfile.mkdtemp()
        for fname, content in token_data.items():
            with open(os.path.join(tmpdir, fname), "w") as f:
                f.write(content if isinstance(content, str) else json.dumps(content))
        client = Garmin()
        client.garth.load(tmpdir)
        # display_name を garth プロファイルから取得（API呼び出し不要）
        try:
            profile = client.garth.profile
            client.display_name = profile.get("displayName") or profile.get("userName") or ""
            print(f"Garmin: display_name={client.display_name}")
        except Exception:
            client.display_name = ""
            print("  display_name をプロファイルから取得できず（無視）")
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

def _truncate(s: str) -> str:
    return s[:CELL_MAX] if len(s) > CELL_MAX else s


def _col_letter(n: int) -> str:
    """1始まりの列番号をA/AA/AB形式に変換。"""
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def _v(d, *keys):
    """ネストしたdict/listから値を安全に取得。整数キーはリストインデックスとして扱う。"""
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k)
        elif isinstance(d, list) and isinstance(k, int):
            d = d[k] if len(d) > k else None
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


def safe_call(client, method_name, *args, **kwargs):
    m = getattr(client, method_name, None)
    if callable(m):
        try:
            return garmin_retry(m, *args, **kwargs)
        except Exception as e:
            print(f"  {method_name} 取得失敗: {e}")
    return None


# ── アクティビティ同期 ─────────────────────────────────────────────────────────

def fetch_activities_for_date(client: Garmin, target_date: datetime) -> list:
    start_local = datetime(target_date.year, target_date.month, target_date.day, tzinfo=JST)
    start_str = (start_local - timedelta(days=1)).strftime("%Y-%m-%d")
    end_str = (start_local + timedelta(days=1)).strftime("%Y-%m-%d")

    activities = garmin_retry(client.get_activities_by_date, startdate=start_str, enddate=end_str)
    picked = []
    for a in activities:
        time_str = a.get("startTimeGMT") or a.get("startTimeLocal") or ""
        dt = parse_start_time(time_str)
        type_key = _v(a, "activityType", "typeKey") or ""
        if dt and dt.astimezone(JST).date() == start_local.date() and "running" in type_key:
            picked.append(a)
    return picked


def get_laps(client: Garmin, activity_id):
    for method_name in ["get_activity_laps", "get_activity_splits"]:
        m = getattr(client, method_name, None)
        if callable(m):
            try:
                result = garmin_retry(m, activity_id)
                if result:
                    return result
            except Exception:
                pass
    return []


def build_activity_row(a: dict, laps) -> list:
    """activity一覧の1件（a）から行データを構築。詳細APIは呼ばない。"""
    start_local_str = a.get("startTimeLocal") or a.get("startTimeGMT") or ""
    date = start_local_str[:10] if start_local_str else ""

    avg_speed = a.get("averageSpeed") or ""
    avg_pace = ""
    if avg_speed and float(avg_speed) > 0:
        avg_pace = round(1000 / 60 / float(avg_speed), 2)

    sport = _v(a, "activityType", "typeKey") or ""

    return [
        date,
        str(a.get("activityId") or ""),
        a.get("activityName") or "",
        sport,
        start_local_str,
        # 距離・時間
        a.get("distance") or "",
        a.get("duration") or a.get("movingDuration") or "",
        a.get("elapsedDuration") or "",
        # スピード・ペース
        avg_speed,
        avg_pace,
        # 心拍
        a.get("averageHR") or "",
        a.get("maxHR") or "",
        # ランニングダイナミクス（一覧に全部ある）
        a.get("averageRunningCadenceInStepsPerMinute") or a.get("averageBikeCadenceInRPM") or "",
        a.get("avgStrideLength") or "",
        a.get("avgVerticalOscillation") or "",
        a.get("avgVerticalRatio") or "",
        a.get("avgGroundContactTime") or "",
        # パワー
        a.get("avgPower") or "",
        a.get("normPower") or "",
        # 標高・消費
        a.get("elevationGain") or "",
        a.get("elevationLoss") or "",
        a.get("calories") or "",
        # トレーニング効果
        a.get("aerobicTrainingEffect") or "",
        a.get("anaerobicTrainingEffect") or "",
        a.get("vO2MaxValue") or "",
        a.get("activityTrainingLoad") or "",
        a.get("differenceBodyBattery") or "",
        # 心拍ゾーン（秒）
        a.get("hrTimeInZone_1") or "",
        a.get("hrTimeInZone_2") or "",
        a.get("hrTimeInZone_3") or "",
        a.get("hrTimeInZone_4") or "",
        a.get("hrTimeInZone_5") or "",
        # ベストスプリット
        a.get("fastestSplit_1000") or "",
        a.get("fastestSplit_5000") or "",
        # 歩数
        a.get("steps") or "",
        # ラップ
        _truncate(json.dumps(laps, ensure_ascii=False) if laps else ""),
        # 生データ
        _truncate(json.dumps(a, ensure_ascii=False)),
    ]


def upsert_activities(ws, activities: list, client: Garmin):
    existing = ws.get_all_values()
    id_to_row = {row[1]: idx + 2 for idx, row in enumerate(existing[1:]) if len(row) >= 2 and row[1]}

    for a in activities:
        activity_id = str(a.get("activityId") or "")
        if not activity_id:
            continue
        laps = get_laps(client, activity_id)
        row = build_activity_row(a, laps)

        if activity_id in id_to_row:
            rn = id_to_row[activity_id]
            ws.update([row], f"A{rn}")
        else:
            ws.append_row(row)


# ── 健康データ同期 ─────────────────────────────────────────────────────────────

def build_health_row(client: Garmin, date_str: str) -> list:
    # 睡眠
    sleep_raw = safe_call(client, "get_sleep_data", date_str) or {}
    sd = sleep_raw.get("dailySleepDTO") or {}
    sleep_start = sd.get("sleepStartTimestampLocal") or ""
    sleep_end = sd.get("sleepEndTimestampLocal") or ""
    if sleep_start and str(sleep_start).lstrip("-").isdigit():
        sleep_start = datetime.fromtimestamp(int(sleep_start) / 1000, tz=JST).isoformat()
    if sleep_end and str(sleep_end).lstrip("-").isdigit():
        sleep_end = datetime.fromtimestamp(int(sleep_end) / 1000, tz=JST).isoformat()

    sleep_score = ""
    scores = sd.get("sleepScores")
    if isinstance(scores, dict):
        sleep_score = _v(scores, "overall", "value")
    if not sleep_score:
        sleep_score = sd.get("sleepScore") or ""

    # HRV
    hrv_raw = safe_call(client, "get_hrv_data", date_str) or {}
    hrv_s = hrv_raw.get("hrvSummary") or {}

    # Body Battery（stress_dataに含まれている。形式: [timestamp, 'MEASURED', value, ...]）
    stress_raw = safe_call(client, "get_stress_data", date_str) or {}
    bb_vals = stress_raw.get("bodyBatteryValuesArray") or []
    bb_nums = [v[2] for v in bb_vals if isinstance(v, list) and len(v) >= 3 and isinstance(v[2], (int, float)) and v[2] >= 0]
    bb_high = max(bb_nums) if bb_nums else ""
    bb_low = min(bb_nums) if bb_nums else ""

    # SpO2
    spo2_raw = safe_call(client, "get_spo2_data", date_str) or {}
    avg_spo2 = spo2_raw.get("averageSpO2") or ""
    min_spo2 = spo2_raw.get("lowestSpO2") or ""

    # 安静時心拍
    rhr_raw = safe_call(client, "get_rhr_day", date_str) or {}
    rhr = _v(rhr_raw, "allMetrics", "metricsMap", "WELLNESS_RESTING_HEART_RATE", 0, "value") or \
          _v(rhr_raw, "allMetrics", "metricsMap", "RESTING_HEART_RATE", 0, "value") or \
          rhr_raw.get("value") or rhr_raw.get("restingHeartRate") or ""

    # 歩数・強度 (get_steps_data)
    steps_raw = safe_call(client, "get_steps_data", date_str) or {}
    # steps_dataはリストの場合がある
    if isinstance(steps_raw, list):
        total_steps = sum(s.get("steps", 0) for s in steps_raw if isinstance(s, dict))
    else:
        total_steps = steps_raw.get("totalSteps") or steps_raw.get("steps") or ""

    # 強度分数はstress_rawかget_stats
    stats_raw = safe_call(client, "get_stats", date_str) or {}
    mod_min = stats_raw.get("moderateIntensityMinutes") or ""
    vig_min = stats_raw.get("vigorousIntensityMinutes") or ""
    active_cal = stats_raw.get("activeKilocalories") or stats_raw.get("activeCalories") or ""

    raw_combined = {
        "sleep": sleep_raw,
        "hrv": hrv_raw,
        "stress_bb": stress_raw,
        "spo2": spo2_raw,
        "rhr": rhr_raw,
        "steps": steps_raw,
        "stats": stats_raw,
    }

    return [
        date_str,
        sleep_start, sleep_end,
        sd.get("sleepTimeSeconds") or "",
        sd.get("deepSleepSeconds") or "",
        sd.get("lightSleepSeconds") or "",
        sd.get("remSleepSeconds") or "",
        sd.get("awakeSleepSeconds") or "",
        sleep_score,
        sd.get("avgHeartRate") or "",
        sd.get("averageRespirationValue") or "",
        sd.get("awakeCount") or "",
        sd.get("avgSleepStress") or "",
        # HRV
        hrv_s.get("weeklyAvg") or "",
        hrv_s.get("lastNight") or hrv_s.get("lastNightAvg") or "",
        hrv_s.get("lastNight5MinHigh") or "",
        # Body Battery
        bb_high, bb_low,
        # ストレス
        stress_raw.get("avgStressLevel") or "",
        stress_raw.get("maxStressLevel") or "",
        # 安静時心拍
        rhr,
        # SpO2
        avg_spo2, min_spo2,
        # 活動量
        total_steps, active_cal, mod_min, vig_min,
        # 生データ
        _truncate(json.dumps(raw_combined, ensure_ascii=False)),
    ]


def upsert_health(ws, date_str: str, row: list):
    existing = ws.get_all_values()
    date_to_row = {r[0]: idx + 2 for idx, r in enumerate(existing[1:]) if r}

    if date_str in date_to_row:
        rn = date_to_row[date_str]
        ws.update([row], f"A{rn}")
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
    ws_activity = ensure_sheet_with_headers(sh, SHEET_ACTIVITY, ACTIVITY_HEADERS)
    ws_health = ensure_sheet_with_headers(sh, SHEET_HEALTH, HEALTH_HEADERS)

    garmin = login_garmin()

    print("アクティビティ取得中...")
    activities = fetch_activities_for_date(garmin, target_date)
    upsert_activities(ws_activity, activities, garmin)
    print(f"  {len(activities)} 件書き込み完了")

    print("健康データ取得中...")
    health_row = build_health_row(garmin, date_str)
    upsert_health(ws_health, date_str, health_row)
    print("  健康データ書き込み完了")

    print(f"完了: {date_str}")


if __name__ == "__main__":
    main()
