hdfs namenode -format -force
hdfs namenode -D dfs.namenode.stale.datanode.interval=10000 -D dfs.namenode.heartbeat.recheck-interval=30000 -D dfs.webhdfs.enabled=true -fs hdfs://main:9000 &
python3 -m jupyterlab --ip=0.0.0.0 --port=5000 --no-browser --allow-root --NotebookApp.token='' 
