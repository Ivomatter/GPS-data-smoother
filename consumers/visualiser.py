import pandas as pd
import folium
from folium.plugins import MarkerCluster
import os

os.makedirs("../maps", exist_ok=True)

raw_data = pd.read_csv('../data/raw_gps.csv')
processed_data = pd.read_csv('../data/processed_gps_data.csv')

raw_gps = raw_data[['veh_id', 'lat', 'lon', 'alt', 'ts']].dropna()
processed_gps = processed_data[['veh_id', 'lat', 'lon', 'alt', 'ts']].dropna()

map_center = [raw_gps['lat'].mean(), raw_gps['lon'].mean()]

colors = ['blue', 'red', 'black', 'orange', 'darkred', 'cadetblue', 'darkgreen']


map_obj_raw = folium.Map(location=map_center, zoom_start=15)
raw_cluster = MarkerCluster(name="Raw Data").add_to(map_obj_raw)


for idx, (veh_id, group) in enumerate(raw_gps.groupby('veh_id')):
    group = group.sort_values(by='ts')  
    coords = group[['lat', 'lon']].values.tolist()
    color = colors[idx % len(colors)]
 
    folium.PolyLine(locations=coords, color=color, weight=2.5, opacity=1, tooltip=f"Raw Vehicle {veh_id}").add_to(map_obj_raw)
    
    
    for _, row in group.iterrows():
        folium.Marker(
            location=[row['lat'], row['lon']],
            popup=f"Veh ID: {veh_id}<br>Alt: {row['alt']}<br>Time: {row['ts']}",
            icon=folium.Icon(color=color, icon='info-sign')
        ).add_to(raw_cluster)

folium.LayerControl().add_to(map_obj_raw)
map_obj_raw.save("../maps/raw_gps_data_map.html")


map_obj_processed = folium.Map(location=map_center, zoom_start=15)
processed_cluster = MarkerCluster(name="Processed Data").add_to(map_obj_processed)


for idx, (veh_id, group) in enumerate(processed_gps.groupby('veh_id')):
    group = group.sort_values(by='ts')  
    coords = group[['lat', 'lon']].values.tolist()
    color = colors[idx % len(colors)]
    

    folium.PolyLine(locations=coords, color=color, weight=2.5, opacity=1, tooltip=f"Processed Vehicle {veh_id}").add_to(map_obj_processed)
    
   
    for _, row in group.iterrows():
        folium.Marker(
            location=[row['lat'], row['lon']],
            popup=f"Veh ID: {veh_id}<br>Alt: {row['alt']}<br>Time: {row['ts']}",
            icon=folium.Icon(color=color, icon='info-sign')
        ).add_to(processed_cluster)

folium.LayerControl().add_to(map_obj_processed)
map_obj_processed.save("../maps/processed_gps_data_map.html")