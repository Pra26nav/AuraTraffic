AuraTraffic dashboard (Python + Streamlit + Hadoop HDFS + Apache Spark)

QUICK START (Windows PowerShell)
1. Open this folder in VS Code.
2. Install Python packages:
   python -m pip install -r requirements.txt
3. Run the dashboard:
   python -m streamlit run app.py
4. Open the local URL printed in the terminal (usually http://localhost:8501).

IMPORTANT
- The dashboard works in demo mode with the included sample roads when HDFS is not configured.
- Spark is used with an RDD map() mapper to transform each road row.
- Hadoop HDFS is storage, not a relational database. For HDFS mode, start Hadoop and upload a CSV with columns source,destination,distance_km,traffic_min.
- In the sidebar, enable 'Try loading from HDFS'. Enter the HDFS path, for example /auratraffic/roads.csv. The app prefixes hdfs://; set your Hadoop default filesystem/configuration if your cluster needs a namenode host and port.
- Java must be installed and JAVA_HOME configured for PySpark. For full HDFS integration, Hadoop must also be running and reachable.
