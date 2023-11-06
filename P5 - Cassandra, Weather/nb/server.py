from concurrent import futures
import grpc
import math
from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.cqltypes import UserType
from cassandra.util import Date
import station_pb2
import station_pb2_grpc
import cassandra
from datetime import date as datetime_date

try:
    cluster = Cluster(['project-5-bencoleman-db-1', 'project-5-bencoleman-db-2', 'project-5-bencoleman-db-3'])
    session = cluster.connect()
except Exception as e:
    print(e)


class StationServicer(station_pb2_grpc.StationServicer):
    def __init__(self):
        self.insert_temps = session.prepare("""
        INSERT INTO weather.stations
        (id, date, record)
        VALUES
        (?, ?, {tmin: ?, tmax: ?})
        """)
        self.insert_temps_lvl = ConsistencyLevel.ONE
        
        self.statement_max = session.prepare("""
        SELECT MAX(record.tmax)
        FROM weather.stations
        WHERE id = ?
        """)
        self.statement_max_lvl = ConsistencyLevel.TWO
        
        
    def RecordTemps(self, request, context):
        self.insert_temps.consistency_level = self.insert_temps_lvl

        try:
            session.execute(self.insert_temps, (request.station, request.date, request.tmin, request.tmax))
            return station_pb2.RecordTempsReply(error = None)
        
        except ValueError:
            return station_pb2.RecordTempsReply(error = "Value error - RecordTemps")
        
        except cassandra.Unavailable:
            return station_pb2.RecordTempsReply(error = "Cassandra unavailable - RecordTemps")
        
        except:
            return station_pb2.RecordTempsReply(error=f"Unknown error: {str(e)}")
                            
    
    def StationMax(self, request, context):
        self.statement_max.consistency_level = self.statement_max_lvl
        
        try:
            result = session.execute(self.statement_max, (request.station,))
            row = result.one()
            max_tmax = row[0] if row[0] is not None else 0
            return station_pb2.StationMaxReply(tmax=max_tmax, error="")
        
        except ValueError:
            return station_pb2.StationMaxReply(tmax=0, error="Value error - StationMax")
        
        except cassandra.Unavailable:
            return station_pb2.StationMaxReply(tmax=0, error="Cassandra unavailable - StationMax")
        
        except Exception as e:
            return station_pb2.StationMaxReply(tmax=0, error=f"Unknown error: {str(e)}")
        

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    station_pb2_grpc.add_StationServicer_to_server(StationServicer(), server)
    server.add_insecure_port("[::]:5440")
    server.start()
    print("Server is running")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
