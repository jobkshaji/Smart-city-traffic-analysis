import pandas as pd
import matplotlib.pyplot as plt


routes = pd.read_csv('routes.txt', sep=',')
stops = pd.read_csv('stops.txt', sep=',')
stop_times = pd.read_csv('stop_times.txt', sep=',')
trips = pd.read_csv('trips.txt', sep=',')

plt.figure(figsize=(8,8))
plt.scatter(stops['stop_lon'],stops['stop_lat'],s=4,alpha=0.5)
plt.title("Delhi bus stop")
plt.xlabel("longitude")
plt.ylabel("latitude")
plt.show()




