import folium.map
import pandas as pd
import matplotlib.pyplot as plt
import folium


routes = pd.read_csv('routes.txt', sep=',')
stops = pd.read_csv('stops.txt', sep=',')
stop_times = pd.read_csv('stop_times.txt', sep=',')
trips = pd.read_csv('trips.txt', sep=',')

merged = stop_times.merge(trips, on="trip_id") \
                   .merge(routes, on="route_id") \
                   .merge(stops, on="stop_id")
#print(merged.head())
busiest_stop= merged.groupby("stop_name").size().sort_values(ascending=False).head(10)
busiest_stop.to_csv("busiest_stops.txt", header=["arrivals"])
#print("Top 10 busiest stop:\n",busiest_stop)

top_stop= busiest_stop.index[0]
stop_data=merged[merged["stop_name"]==top_stop]

#converting arival time into hours
stop_data["hours"]=pd.to_datetime(stop_data["arrival_time"],format='%H:%M:%S',errors="coerce").dt.hour

arrival_by_hour=stop_data.groupby("hours").size()
#print(arrival_by_hour.head)

plt.figure(figsize=(10,5))
arrival_by_hour.plot(kind="bar" , color="skyblue")
plt.title(f"Arrivals per hour - {top_stop}")
plt.xlabel("hours of day")
plt.ylabel("number of arrival")
plt.grid(axis="y")
plt.tight_layout()
plt.savefig("arrivals_per_hour.png")
plt.show()

delhi_map = folium.Map(location=[28.6139, 77.2090], zoom_start=12)
for stop_name,count in busiest_stop.items():
    lat=stops.loc[stops["stop_name"]==stop_name,"stop_lat"].values[0]
    lon=stops.loc[stops["stop_name"]==stop_name,"stop_lon"].values[0]
    folium.Marker(
        [lat,lon],
        popup=f"{stop_name}-{count}arrivals"
    ).add_to(delhi_map)
delhi_map.save("busiest_stops_map.html")
print("Saved busiest_stops_map.html — open it in your browser")