import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import folium
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# Load the pre-processed data with location names
top10 = pd.read_csv("top_congested_roads.csv")

# Create Point geometries for the top10 data
geometry = [Point(xy) for xy in zip(top10.lon, top10.lat)]
top10 = gpd.GeoDataFrame(top10, geometry=geometry, crs="EPSG:4326")

# Create a shortened location name for the popup
#top10['short_location_name'] = top10['location_name'].apply(lambda x: x.split(',')[0] if isinstance(x, str) else 'Unknown')


print(top10[["lat", "lon", "slowdown", "location_name"]])

# Center map at Delhi
m = folium.Map(location=[28.6139, 77.2090], zoom_start=11, tiles="cartodbpositron")

for _, row in top10.iterrows():
    color = "red" if row["slowdown"] > 20 else "orange" if row["slowdown"] > 10 else "green"
    folium.CircleMarker(
        location=[row["lat"], row["lon"]],
        radius=6,
        popup=f"Location: {row['location_name']} Slowdown: {row['slowdown']:.1f} km/h",
        color=color,
        fill=True,
        fill_opacity=0.8
    ).add_to(m)

# Save
m.save("delhi_top10_congestion_map.html")