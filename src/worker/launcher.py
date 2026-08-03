import multiprocessing
import os
import time
import signal
import sys
from src.worker.main import DistributedCrawlerWorker, REDIS_URL

# Set the exact number of parallel workers
NUM_WORKERS = 5

def run_worker_process(worker_id: int):
    """Target function for each process. Runs a dedicated asyncio event loop."""
    import asyncio
    import logging

    # Configure logging so we can easily tell which worker is printing
    logging.basicConfig(
        level=logging.INFO,
        format=f"[Worker-{worker_id}] %(asctime)s - %(message)s",
        force=True # Overwrite default root logger config
    )

    logging.info(f"Worker process starting on PID {os.getpid()}...")

    # Instantiate and run our crawler worker
    worker = DistributedCrawlerWorker(REDIS_URL)
    try:
        asyncio.run(worker.run())
    except KeyboardInterrupt:
        logging.info("Worker process stopping gracefully...")

def signal_handler(sig, frame):
    """Ensures that pressing Ctrl+C terminates all 5 workers cleanly."""
    print("\n[Launcher] Shutdown signal received. Terminating all 5 workers...")
    sys.exit(0)

if __name__ == "__main__":
    # Register the interrupt handler in the main process
    signal.signal(signal.SIGINT, signal_handler)

    print(f"[Launcher] Preparing to launch {NUM_WORKERS} parallel crawler processes...")
    processes = []

    # Spin up exactly 5 parallel worker processes
    for i in range(1, NUM_WORKERS + 1):
        p = multiprocessing.Process(target=run_worker_process, args=(i,), daemon=True)
        processes.append(p)
        p.start()
        # Small delay so they don't hit Redis connection limit all at the exact same millisecond
        time.sleep(0.2)

    print(f"[Launcher] All {NUM_WORKERS} workers are live! Press Ctrl+C to terminate the cluster.")

    # Keep the main process alive while the workers run
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
