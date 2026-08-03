import sys
import time
from pathlib import Path

# `streamlit run src/dashboard.py` puts this file's own directory on
# sys.path, not the project root, so `import src.config` needs a hand.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import redis
import streamlit as st

import src.config as config

# Configure the Streamlit layout to take up the full screen width
st.set_page_config(
    page_title="🕷️ Distributed Crawler Monitor",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Connect to the shared Redis instance
r = redis.from_url(config.REDIS_URL, decode_responses=True)

# Main Title and Controls
st.title("🕷️ Distributed Web Crawler Live Monitor")
st.markdown("---")

# Layout: 3 Top-Level Metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    queue_metric = st.empty()
with col2:
    pages_metric = st.empty()
with col3:
    workers_metric = st.empty()
with col4:
    locks_metric = st.empty()

st.markdown("### Active Domain Politeness Locks")
lock_table_placeholder = st.empty()

st.markdown("### Active Worker Registry")
worker_list_placeholder = st.empty()

# Sidebar seed controller so recruiters can launch tasks from the UI
with st.sidebar:
    st.header("Controller")
    seed_url = st.text_input("Seed URL", "https://news.ycombinator.com")
    if st.button("Enqueue Seed URL"):
        # Frontier entries are "url|depth" -> priority (depth), matching crawler.py
        r.zadd("crawler:frontier", {f"{seed_url}|0": 0})
        st.success(f"Enqueued {seed_url}!")

# Live Polling Loop
while True:
    try:
        # 1. Fetch KPI Metrics from Redis
        queue_size = r.zcard("crawler:frontier")
        pages_crawled = r.get("crawler:pages_crawled") or 0
        
        # Scan for active worker heartbeats
        worker_keys = r.keys("worker:heartbeat:*")
        active_workers = len(worker_keys)
        
        # Scan for active locks
        lock_keys = r.keys("lock:*")
        active_locks = len(lock_keys)
        
        # Update metric cards dynamically
        queue_metric.metric("URLs in Queue", f"{queue_size:,}")
        pages_metric.metric("Total Pages Crawled", f"{int(pages_crawled):,}")
        workers_metric.metric("Active Workers", f"{active_workers}")
        locks_metric.metric("Active Domain Locks", f"{active_locks}")
        
        # 2. Render Active Domain Locks with remaining TTL
        lock_data = []
        for key in lock_keys:
            domain = key.replace("lock:", "")
            ttl = r.ttl(key)
            lock_data.append({"Domain": domain, "Time Remaining (Seconds)": f"{ttl}s"})
            
        if lock_data:
            df_locks = pd.DataFrame(lock_data)
            lock_table_placeholder.table(df_locks)
        else:
            lock_table_placeholder.info("No active politeness locks. Workers are free to crawl any domain.")

        # 3. Render Active Worker Heartbeats
        if worker_keys:
            worker_ids = [key.replace("worker:heartbeat:", "") for key in worker_keys]
            worker_df = pd.DataFrame({"Worker Process ID": worker_ids, "Status": ["🟢 Online" for _ in worker_ids]})
            worker_list_placeholder.dataframe(worker_df, width="stretch")
        else:
            worker_list_placeholder.warning("No workers detected. Spin up worker.py processes to begin processing.")

    except redis.exceptions.ConnectionError:
        st.error("Cannot connect to Redis. Make sure Redis is running locally on port 6379.")
        break
        
    # Block for 1 second before fetching new metrics
    time.sleep(1)