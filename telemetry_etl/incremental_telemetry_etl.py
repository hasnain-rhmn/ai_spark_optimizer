import requests
import duckdb
import pandas as pd
import time
import os
import re

# --- CONFIGURATION ---\
DB_DIR = "../database"
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "spark_telemetry.duckdb")

BASE_HIST_URL = "http://localhost:18080/api/v1/applications"
PROM_URL = "http://localhost:9090/api/v1/query_range"

TARGET_METRICS = [
    "jvm_heap_used", "jvm_pools_G1_Old_Gen_used", "ExecutorMetrics_JVMOffHeapMemory",
    "ExecutorMetrics_ProcessTreePythonRSSMemory", "ExecutorMetrics_TotalGCTime",
    "ExecutorMetrics_MajorGCTime", "BlockManager_memory_memUsed_MB", 
    "BlockManager_disk_diskSpaceUsed_MB"
]

# --- HELPER: PLAN CLEANING STRATEGY ---
def clean_physical_plan(plan):
    """Strips IDs, Codegen, and truncates massive column lists for LLM readability."""
    if not plan: return ""
    plan = re.sub(r'\[codegen id : \d+\]', '', plan)
    plan = re.sub(r'#\d+', '#ID', plan)
    def shorten_list(match):
        items = match.group(1).split(', ')
        return f"[{', '.join(items[:3])}, ..., {items[-1]}]" if len(items) > 5 else match.group(0)
    plan = re.sub(r'\[(.*?)\]', shorten_list, plan)
    lines = [l.strip() for l in plan.split('\n') if any(m in l for m in ['+-', ':', '(', '*', '=='])]
    return "\n".join(lines[:50])

# --- DATABASE SETUP ---
def initialize_database(con):
    print("🛠️  Resetting Schema (Dev Mode)...")
    con.execute("CREATE OR REPLACE TABLE dim_applications (app_id VARCHAR, app_name VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP, duration_ms BIGINT, final_status VARCHAR);")
    con.execute("CREATE OR REPLACE TABLE fact_stages (app_id VARCHAR, stage_id INTEGER, start_time TIMESTAMP, end_time TIMESTAMP, num_tasks INTEGER, executor_run_time_ms BIGINT, input_bytes_kb DOUBLE, shuffle_read_kb DOUBLE, shuffle_write_kb DOUBLE, memory_spill_kb DOUBLE, disk_spill_kb DOUBLE, task_duration_median_ms BIGINT, task_duration_max_ms BIGINT);")
    con.execute("CREATE OR REPLACE TABLE fact_executors (app_id VARCHAR, executor_id VARCHAR, host_ip VARCHAR, total_cores INTEGER, max_memory_kb DOUBLE, is_active BOOLEAN, exit_reason VARCHAR);")
    con.execute("CREATE OR REPLACE TABLE dim_sql_plans (app_id VARCHAR, sql_id INTEGER, description VARCHAR, plan_text TEXT, duration_ms BIGINT);")
    con.execute("CREATE OR REPLACE TABLE fact_hardware_metrics (app_id VARCHAR, timestamp TIMESTAMP, executor_id VARCHAR, metric_name VARCHAR, value_kb_or_ms DOUBLE);")
    con.execute("CREATE OR REPLACE TABLE map_jobs (app_id VARCHAR, job_id INTEGER, sql_id INTEGER, stage_ids VARCHAR);")

# --- EXTRACTION LOGIC ---
def fetch_app_data(app_id):
    resp = requests.get(f"{BASE_HIST_URL}/{app_id}")
    if resp.status_code != 200: return pd.DataFrame(), None, None
    data = resp.json()
    attempt = data['attempts'][0]
    start_dt = pd.to_datetime(attempt.get('startTime'), utc=True)
    end_dt = pd.to_datetime(attempt.get('endTime'), utc=True) if attempt.get('completed') else pd.Timestamp.utcnow()
    df = pd.DataFrame([{
        "app_id": app_id, "app_name": data['name'], "start_time": start_dt.tz_localize(None),
        "end_time": end_dt.tz_localize(None), "duration_ms": attempt.get('duration', 0), "final_status": "SUCCEEDED"
    }])
    return df, start_dt.timestamp(), end_dt.timestamp()

def fetch_stages(app_id):
    """Fetches stages and drills into /taskSummary for p50/p100 distributions."""
    resp = requests.get(f"{BASE_HIST_URL}/{app_id}/stages")
    if resp.status_code != 200: return pd.DataFrame()
    
    stage_list = resp.json()
    rows = []
    for s in stage_list:
        sid, aid = s['stageId'], s.get('attemptId', 0)
        
        # Drill down for distribution metrics
        sum_url = f"{BASE_HIST_URL}/{app_id}/stages/{sid}/{aid}/taskSummary?quantiles=0,0.5,1.0"
        sum_resp = requests.get(sum_url)
        median_ms, max_ms = 0, 0
        
        if sum_resp.status_code == 200:
            dist = sum_resp.json().get('executorRunTime', [])
            if len(dist) >= 3:
                median_ms, max_ms = dist[1], dist[2] # Index 1 is 0.5, Index 2 is 1.0 based on our 3 quantiles request
        
        rows.append({
            "app_id": app_id, "stage_id": sid,
            "start_time": pd.to_datetime(s.get('submissionTime'), utc=True).tz_localize(None) if s.get('submissionTime') else None,
            "end_time": pd.to_datetime(s.get('completionTime'), utc=True).tz_localize(None) if s.get('completionTime') else None,
            "num_tasks": s['numTasks'], "executor_run_time_ms": s.get('executorRunTime', 0),
            "input_bytes_kb": round(s.get('inputBytes', 0) / 1024, 2),
            "shuffle_read_kb": round(s.get('shuffleReadBytes', 0) / 1024, 2),
            "shuffle_write_kb": round(s.get('shuffleWriteBytes', 0) / 1024, 2),
            "memory_spill_kb": round(s.get('memoryBytesSpilled', 1) / 1024, 2),
            "disk_spill_kb": round(s.get('diskBytesSpilled', 0) / 1024, 2),
            "task_duration_median_ms": int(median_ms), "task_duration_max_ms": int(max_ms)
        })
    return pd.DataFrame(rows)

def fetch_sql_and_job_maps(app_id):
    sql_rows, job_map_dict = [], {}
    j_resp = requests.get(f"{BASE_HIST_URL}/{app_id}/jobs")
    job_to_stages = {j['jobId']: ",".join(map(str, j.get('stageIds', []))) for j in j_resp.json()} if j_resp.status_code == 200 else {}
    
    if j_resp.status_code == 200:
        for j in j_resp.json():
            jid = j['jobId']
            sql_id = j.get('sqlExecutionId')
            job_map_dict[jid] = {"app_id": app_id, "job_id": jid, "sql_id": int(sql_id) if sql_id is not None else None, "stage_ids": job_to_stages.get(jid, "")}

    s_resp = requests.get(f"{BASE_HIST_URL}/{app_id}/sql")
    if s_resp.status_code == 200:
        for s in s_resp.json():
            sid = s['id']
            d_resp = requests.get(f"{BASE_HIST_URL}/{app_id}/sql/{sid}")
            if d_resp.status_code == 200:
                d = d_resp.json()
                plan = clean_physical_plan(d.get('planDescription') or d.get('physicalPlanDescription') or "")
                for jid in (d.get('successJobIds', []) + d.get('failedJobIds', []) + d.get('runningJobIds', [])):
                    if jid in job_map_dict: job_map_dict[jid]["sql_id"] = sid
                    else: job_map_dict[jid] = {"app_id": app_id, "job_id": jid, "sql_id": sid, "stage_ids": job_to_stages.get(jid, "")}
            else: plan = ""
            sql_rows.append({"app_id": app_id, "sql_id": sid, "description": s.get('description', ''), "plan_text": plan, "duration_ms": s.get('duration', 0)})

    return pd.DataFrame(sql_rows), pd.DataFrame(list(job_map_dict.values()))

def fetch_hardware_metrics(app_id, start_ts, end_ts):
    fmt_id = app_id.replace("-", "_")
    regex = f'{{__name__=~"spark_{fmt_id}_.*({"|".join(TARGET_METRICS)})$"}}'
    resp = requests.get(PROM_URL, params={'query': regex, 'start': start_ts - 5, 'end': end_ts + 5, 'step': '1s'}).json()
    rows = []
    for res in resp.get('data', {}).get('result', []):
        name = res['metric']['__name__']
        metric = "_".join(name.split(f"spark_{fmt_id}_")[1].split("_")[1:])
        exec_id = name.split(f"spark_{fmt_id}_")[1].split("_")[0]
        for val in res['values']:
            v = float(val[1])
            final_v = round(v, 2) if "Time" in name else (round(v * 1024, 2) if "_MB" in name else round(v / 1024, 2))
            rows.append({"app_id": app_id, "timestamp": pd.to_datetime(val[0], unit='s', utc=True).tz_localize(None), "executor_id": exec_id, "metric_name": metric, "value_kb_or_ms": final_v})
    return pd.DataFrame(rows)

# --- ORCHESTRATION ---
def process_single_app(con, app_id):
    print(f"🚀 Processing: {app_id}")
    df_app, start_ts, end_ts = fetch_app_data(app_id)
    if df_app.empty: return
    df_sql, df_map = fetch_sql_and_job_maps(app_id)
    
    e_resp = requests.get(f"{BASE_HIST_URL}/{app_id}/allexecutors")
    df_execs = pd.DataFrame([{"app_id": app_id, "executor_id": e['id'], "host_ip": e['hostPort'].split(':')[0], "total_cores": e['totalCores'], "max_memory_kb": round(e['maxMemory'] / 1024, 2), "is_active": e['isActive'], "exit_reason": e.get('executorLogs', {}).get('exitReason', 'N/A')} for e in e_resp.json()]) if e_resp.status_code == 200 else pd.DataFrame()

    dfs = {"dim_applications": df_app, "fact_stages": fetch_stages(app_id), "fact_executors": df_execs, "map_jobs": df_map, "dim_sql_plans": df_sql, "fact_hardware_metrics": fetch_hardware_metrics(app_id, start_ts, end_ts)}
    
    con.execute("BEGIN TRANSACTION")
    try:
        for table, df in dfs.items():
            if not df.empty: con.execute(f"INSERT INTO {table} SELECT * FROM df")
        con.execute("COMMIT")
        print(f"✅ App {app_id} Loaded")
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    con = duckdb.connect(DB_PATH)
    try:
        initialize_database(con)
        apps = requests.get(BASE_HIST_URL).json()
        for a in apps:
            if a['attempts'][0].get('completed'): process_single_app(con, a['id'])
    finally:
        con.close()