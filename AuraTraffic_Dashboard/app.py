import streamlit as st
import pandas as pd
import networkx as nx
from pyspark.sql import SparkSession

st.set_page_config(page_title="AuraTraffic | Smart Mobility", page_icon="🚦", layout="wide")
st.markdown("""
<style>
.stApp{background:linear-gradient(135deg,#07111f,#101d32);color:#edf4ff}
[data-testid="stMetric"]{background:#14243b;padding:18px;border:1px solid #263d5c;border-radius:14px}
section[data-testid="stSidebar"]{background:#0b1728}
</style>
""", unsafe_allow_html=True)
st.title("🚦 AuraTraffic")
st.caption("SMART ROUTE INTELLIGENCE  ·  Hadoop HDFS + Apache Spark + Python")

# Demo data is used when HDFS is not configured or available.
DEMO = pd.DataFrame([
    ("College", "Airport", 18, 35), ("College", "City Center", 7, 14),
    ("City Center", "Airport", 12, 22), ("City Center", "Railway Station", 5, 11),
    ("Railway Station", "Airport", 10, 20), ("College", "Hospital", 4, 8),
    ("Hospital", "City Center", 6, 12), ("Hospital", "Railway Station", 8, 15),
    ("Airport", "Tech Park", 14, 25), ("City Center", "Tech Park", 9, 16),
], columns=["source", "destination", "distance_km", "traffic_min"])

@st.cache_resource
def spark_session():
    return SparkSession.builder.appName("AuraTrafficMapper").master("local[*]").getOrCreate()

with st.sidebar:
    st.header("⚙️ Data settings")
    hdfs_path = st.text_input("HDFS CSV path (optional)", "/auratraffic/roads.csv")
    st.caption("CSV columns: source,destination,distance_km,traffic_min")
    use_hdfs = st.toggle("Try loading from HDFS", value=False)
    st.divider()
    st.markdown("**Pipeline**\n\n📦 HDFS stores road data\n\n⚡ Spark maps traffic records\n\n🧭 Python finds the best route")

roads, data_source, spark_status = DEMO.copy(), "Built-in demo data", "Not started"
spark = None
try:
    spark = spark_session()
    spark_status = "Running (local Spark)"
    if use_hdfs:
        try:
            roads = spark.read.option("header", True).option("inferSchema", True).csv("hdfs://" + hdfs_path.lstrip("/")) .toPandas()
            roads.columns = [c.lower().strip() for c in roads.columns]
            data_source = "Hadoop HDFS"
        except Exception as e:
            st.warning("HDFS could not be read. Showing demo data instead. Check Hadoop is running and the HDFS path exists.")
except Exception:
    spark_status = "Unavailable — install Java and PySpark"

required = {"source", "destination", "distance_km", "traffic_min"}
if not required.issubset(set(roads.columns)):
    st.error("CSV needs columns: source, destination, distance_km, traffic_min")
    st.stop()
roads["source"] = roads["source"].astype(str)
roads["destination"] = roads["destination"].astype(str)

# Spark Mapper: each input row becomes a compact (source, destination, travel-time) record.
try:
    rdd = spark.sparkContext.parallelize(roads.to_dict("records"))
    mapped = rdd.map(lambda r: (str(r["source"]), str(r["destination"]), float(r["traffic_min"]))).collect()
    mapper_status = f"Spark map() processed {len(mapped)} road records"
except Exception:
    mapped = list(roads[["source", "destination", "traffic_min"]].itertuples(index=False, name=None))
    mapper_status = f"Python fallback mapped {len(mapped)} road records"

c1,c2,c3,c4 = st.columns(4)
c1.metric("Road connections", len(roads))
c2.metric("Locations", len(set(roads.source) | set(roads.destination)))
c3.metric("Average travel time", f"{roads.traffic_min.mean():.1f} min")
c4.metric("Data source", data_source)
st.markdown("### 🗺️ Route planner")
locations = sorted(set(roads.source) | set(roads.destination))
a,b = st.columns(2)
source = a.selectbox("Starting point", locations, index=locations.index("College") if "College" in locations else 0)
destination = b.selectbox("Destination", [x for x in locations if x != source], index=0)
metric = st.radio("Optimize route for", ["Fastest (traffic minutes)", "Shortest (kilometres)"], horizontal=True)
weight = "traffic_min" if metric.startswith("Fastest") else "distance_km"
G = nx.Graph()
for _, r in roads.iterrows():
    G.add_edge(r.source, r.destination, weight=float(r[weight]), minutes=float(r.traffic_min), km=float(r.distance_km))
if st.button("✨ Find best route", type="primary", use_container_width=True):
    try:
        route = nx.shortest_path(G, source, destination, weight="weight")
        legs = [G[u][v] for u,v in zip(route, route[1:])]
        x,y,z = st.columns(3)
        x.metric("Estimated time", f"{sum(e['minutes'] for e in legs):.0f} min")
        y.metric("Total distance", f"{sum(e['km'] for e in legs):.1f} km")
        z.metric("Route stops", len(route))
        st.success("  →  ".join(route))
        st.dataframe(pd.DataFrame([{"From":u,"To":v,"Distance (km)":G[u][v]["km"],"Traffic (min)":G[u][v]["minutes"]} for u,v in zip(route,route[1:])]), use_container_width=True, hide_index=True)
    except nx.NetworkXNoPath:
        st.error("No route connects these locations.")

left,right = st.columns([1.3,1])
with left:
    st.markdown("### 📊 Road network data")
    st.dataframe(roads, use_container_width=True, hide_index=True)
with right:
    st.markdown("### ⚡ Big-data pipeline")
    st.info(f"**Hadoop HDFS**\n\n{data_source}")
    st.info(f"**Apache Spark**\n\n{spark_status}\n\n{mapper_status}")
    st.caption("Demo mode uses sample data. For real HDFS integration, start Hadoop, upload roads.csv, and enable the HDFS toggle.")

st.markdown("---")
st.caption("AuraTraffic · Academic project demo · Python • Streamlit • Hadoop HDFS • Apache Spark • Dijkstra")
