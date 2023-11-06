from concurrent import futures
import threading
import math
import time
import grpc
import numstore_pb2
import numstore_pb2_grpc

class NumStoreServicer(numstore_pb2_grpc.NumStoreServicer):
    def __init__(self):
        self.values_sum = 0
        self.values = {}
        self.lock = threading.Lock()
        self.cache = {} # FIFO Eviction Policy (Oldest in the queue is removed)
        self.cache_queue = []
        
        print("Server is running ")
        
    def SetNum(self, request, context):
        with self.lock:
            key = request.key
            value = request.value
            
            if key in self.values:
                self.values_sum -= self.values[key]

            self.values[request.key] = value
            self.values_sum += value
            return(numstore_pb2.SetNumResponse(total = self.values_sum))

    def Fact(self, request, context):
        
        hit = False
        key = request.key
        
        with self.lock:
            if key not in self.values:
                return numstore_pb2.FactResponse(error=f"Key: {key} not found.") 
            value = self.values[key]       

        if key in self.cache:
            hit = True
            with self.lock:
                factor = self.cache[key]
        else:
            factor = math.factorial(value)
            with self.lock:
                self.cache[key] = factor
                self.cache_queue.append(key)
                if len(self.cache_queue) > 10:
                    remove = self.cache_queue.pop(0)
                    del self.cache[remove]

        return numstore_pb2.FactResponse(value = factor, hit = hit)
    

server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
numstore_pb2_grpc.add_NumStoreServicer_to_server(NumStoreServicer(), server)
server.add_insecure_port("[::]:5440")
server.start()
server.wait_for_termination()