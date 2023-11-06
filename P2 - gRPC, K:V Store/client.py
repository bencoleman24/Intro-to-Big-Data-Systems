import time
import numpy as np
import argparse
import random
import sys
import threading
import grpc
import numstore_pb2, numstore_pb2_grpc
from concurrent import futures

lock = threading.Lock()

parser = argparse.ArgumentParser()
parser.add_argument('port', type = str)
port = parser.parse_args().port

hits_total = 0
misses_total = 0
thread_total = 8
keys_total = 100
request_total = 100
times = []

# Create list of numbers between 1 and 15
numbers = list(range(2,15))

# Create list of 100 keys
keys = set()
while len(keys) < keys_total:
    key = ''.join(chr(random.randint(0, 25) + ord('A')) for _ in range(3))
    keys.add(key)
keys = list(keys)

# Connect to server
channel = grpc.insecure_channel('localhost:' + port)
stub = numstore_pb2_grpc.NumStoreStub(channel)

def run_thread():
    global hits_total
    global misses_total
    global times
    # Start making requests
    for i in range(request_total):
        key = random.choice(keys)
        if random.choice([True, False]): 
            value = random.choice(numbers)
            with lock:
                start_time = time.time()
                response = stub.SetNum(numstore_pb2.SetNumRequest(key=key, value=value))
                end_time = time.time()
        else:
            with lock:
                start_time = time.time()
                response = stub.Fact(numstore_pb2.SetNumRequest(key=key))
                end_time = time.time()
            if response.hit:
                hits_total += 1
            else:
                misses_total += 1
        times.append(end_time - start_time)


# Start the threads
threads = []
for i in range(thread_total):
    thread = threading.Thread(target = run_thread)
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()


p50 = np.percentile(times, 50)
p99 = np.percentile(times, 99)
hit_rate = hits_total / (hits_total + misses_total)

print('Cache hit rate: ' + str(hit_rate))
print("50th percentile response time (sec): " + str(p50))
print("99th percentile response time (sec): " + str(p99))
