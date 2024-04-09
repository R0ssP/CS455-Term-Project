import folium
map = folium.Map([40.7580,-73.9855], zoom_start=10.5)

folium.Marker([40.6000, -74.000]).add_to(map)

map.save('./maps/map_try1.html')