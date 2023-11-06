FROM ubuntu:22.10
RUN apt-get update

FROM python:3.9
WORKDIR /app
COPY numstore.proto /app
COPY server.py /app
COPY numstore_pb2.py /app
COPY numstore_pb2_grpc.py /app
RUN pip install grpcio grpcio-tools
EXPOSE 5440
CMD ["python", "server.py"]
