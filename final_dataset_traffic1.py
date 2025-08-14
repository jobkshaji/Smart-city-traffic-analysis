import requests
import pandas as pd
import folium
from folium.plugins import HeatMap
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time
# ========================
# CONFIG
# ========================
API_KEY = "6oV3YgopwRtfRyXufczW4YLDYYDNEqbx"

# Delhi bounding box
MIN_LAT, MAX_LAT = 28.40, 28.88
MIN_LON, MAX_LON = 76.84, 77.34
STEP = 0.05  # smaller step = more points but more API calls


# FUNCTION: Fetch traffic for one point

def fetch_tomtom_traffic(lat, lon):
    url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
    params = {"point": f"{lat},{lon}", "key": API_KEY}
    try:
        r = requests.get(url, params=params)
        if r.status_code == 200:
            data = r.json()
            fsd = data.get("flowSegmentData", {})
            return {
                "lat": lat,
                "lon": lon,
                "currentSpeed": fsd.get("currentSpeed"),
                "freeFlowSpeed": fsd.get("freeFlowSpeed")
            }
    except Exception as e:
        print("Error:", e)
    return None

# STEP 1: Collect traffic data

traffic_data = []
lat = MIN_LAT
while lat <= MAX_LAT:
    lon = MIN_LON
    while lon <= MAX_LON:
        traffic = fetch_tomtom_traffic(lat, lon)
        if traffic:
            traffic_data.append(traffic)
        lon += STEP
        time.sleep(0.1)  # avoid hitting API too fast
    lat += STEP

df = pd.DataFrame(traffic_data)
df['slowdown'] = df['freeFlowSpeed'] - df['currentSpeed']
df.to_csv("delhi_traffic.csv", index=False)
print(f" Collected {len(df)} points of live traffic data.")


# STEP 2: Generate heatmap

m = folium.Map(location=[28.6139, 77.2090], zoom_start=10, tiles="cartodbpositron")
heat_data = [[row['lat'], row['lon'], row['slowdown']] for _, row in df.iterrows()]
HeatMap(heat_data, min_opacity=0.4, radius=15, blur=15, max_zoom=1).add_to(m)
m.save("delhi_traffic_heatmap.html")
print(" Heatmap saved: delhi_traffic_heatmap.html")


# STEP 3: Find top congested points (reverse geocoding)

top_congested = df.sort_values(by="slowdown", ascending=False).head(10).copy()
geolocator = Nominatim(user_agent="delhi_traffic_app")
reverse = RateLimiter(geolocator.reverse, min_delay_seconds=1)

locations = []
for lat, lon in zip(top_congested['lat'], top_congested['lon']):
    try:
        loc = reverse((lat, lon), exactly_one=True)
        locations.append(loc.address if loc else "Unknown location")
    except:
        locations.append("Error")

top_congested['location_name'] = locations
top_congested.to_csv("top_congested_roads.csv", index=False)
print(" Top congested points saved: top_congested_roads.csv")
