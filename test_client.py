import threading
import time
import sys
import os
import random
from client import TupleSpaceClient

def run_client(host, port, request_file, client_id, delay=0):
    """Run a client instance with optional startup delay"""
    time.sleep(delay)  # Apply startup delay
    print(f"\n[Client {client_id}] Starting with request file: {request_file}")
    
    start_time = time.time()
    client = TupleSpaceClient(host, port, request_file)
    status = "UNKNOWN"
    
    try:
        if client.connect():
            client.process_requests()
            status = "SUCCESS"
        else:
            status = "CONNECTION FAILED"
    except Exception as e:
        status = f"FAILED: {str(e)}"
    
    elapsed = time.time() - start_time
    print(f"[Client {client_id}] {status} in {elapsed:.2f} seconds")
    return status

def generate_test_files(num_files=5, requests_per_file=100):
    """Generate test request files with random operations"""
    # Create requests directory if needed
    if not os.path.exists("requests"):
        os.makedirs("requests")
    
    # Sample data for keys and values
    keys = ["TeamA", "TeamB", "TeamC", "TeamD", "TeamE"]
    values = ["Champion", "Runner-up", "Third", "Fourth", "Fifth"]
    
    for i in range(1, num_files + 1):
        filename = f"requests/client{i}.txt"
        with open(filename, "w") as f:
            for _ in range(requests_per_file):
                # Randomly select operation type
                op = random.choice(["PUT", "GET", "READ"])
                key = random.choice(keys)
                
                # Format request based on operation
                if op == "PUT":
                    value = random.choice(values)
                    f.write(f"{op} {key}{i} {value}{i}\n")
                else:
                    f.write(f"{op} {key}{i}\n")
        
        print(f"Generated test file: {filename}")

def sequential_test(host, port, num_clients=3):
    """Run clients sequentially (one after another)"""
    print("\n" + "="*70)
    print("RUNNING CLIENTS SEQUENTIALLY")
    print("="*70)
    
    results = []
    for i in range(1, num_clients + 1):
        # Run client with no delay
        result = run_client(host, port, f"requests/client{i}.txt", i)
        results.append(result)
    
    # Print summary
    print("\nSequential Test Summary:")
    for i, res in enumerate(results, 1):
        print(f"Client {i}: {res}")

def parallel_test(host, port, num_clients=5, max_delay=1.0):
    """Run clients concurrently with random startup delays"""
    print("\n" + "="*70)
    print("RUNNING CLIENTS IN PARALLEL")
    print("="*70)
    
    threads = []
    results = [None] * num_clients  # Store results by index
    
    for i in range(1, num_clients + 1):
        # Random delay to simulate real-world conditions
        delay = random.uniform(0, max_delay)
        # Create thread with client task
        thread = threading.Thread(
            target=lambda idx=i: results.__setitem__(
                idx-1, 
                run_client(host, port, f"requests/client{idx}.txt", idx, delay)
            )
        )
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    # Print summary
    print("\nParallel Test Summary:")
    for i, res in enumerate(results, 1):
        print(f"Client {i}: {res}")

def stress_test(host, port, num_clients=10, duration=30):
    """Run clients continuously for a specified duration"""
    print("\n" + "="*70)
    print(f"STRESS TEST: {num_clients} CLIENTS FOR {duration} SECONDS")
    print("="*70)
    
    stop_event = threading.Event()  # Signal to stop threads
    threads = []
    
    def client_worker(client_id):
        """Worker function for stress test clients"""
        while not stop_event.is_set():
            # Select random request file
            file_id = random.randint(1, 5)
            try:
                # Run client with random file
                run_client(host, port, f"requests/client{file_id}.txt", client_id)
            except Exception:
                pass  # Ignore errors in stress test
            # Random delay between requests
            time.sleep(random.uniform(0.1, 0.5))
    
    # Create and start worker threads
    for i in range(num_clients):
        thread = threading.Thread(target=client_worker, args=(i+1,))
        thread.daemon = True  # Terminate with main thread
        threads.append(thread)
        thread.start()
    
    # Run test for specified duration
    time.sleep(duration)
    # Signal threads to stop
    stop_event.set()
    
    # Wait for threads to finish
    for thread in threads:
        thread.join(timeout=1.0)
    
    print("\nStress test completed")