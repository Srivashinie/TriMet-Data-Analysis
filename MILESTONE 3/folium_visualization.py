import folium

# Load GeoJSON data
geojson_data = 'visu5c.geojson'
# Create a map centered around a specific location
m = folium.Map(location=[45.511212, -122.683083], zoom_start=13)

# Add GeoJSON data to the map
folium.GeoJson(geojson_data).add_to(m)

# Save to an HTML file
m.save('map523.html')