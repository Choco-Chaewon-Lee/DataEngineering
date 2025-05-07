import folium
from folium import plugins
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
import seaborn as sns
import matplotlib.colors as mcolors

from pymongo import MongoClient

client = MongoClient("")
db = client["dataEngineering"]
col = db["GoogleMap_jeju"]

docs = list(col.find({
    'location': {'$exists': True},
    'avg_rating': {'$ne': None},
    'tour_name': {'$exists': True} 
}, {
    '_id': 0,
    'tour_name': 1,
    'avg_rating': 1,
    'review_count' : 1,
    'location.coordinates': 1,
}))

# 좌표와 평점 추출
data = []
for doc in docs:
    coords = doc.get('location', {}).get('coordinates')
    if (coords and len(coords) == 2 
        and all(isinstance(v, (int, float)) for v in coords)
        and 'tour_name' in doc):
        
        lon, lat = coords
        data.append({
            'tour_name': doc['tour_name'],
            'latitude': lat,
            'longitude': lon,
            'review_count': doc.get('review_count', 0),
            'avg_rating': doc.get('avg_rating', 0),
        })

df = pd.DataFrame(data)
df = df.dropna(subset=['latitude', 'longitude']).reset_index(drop=True)

k = 6
kmeans = KMeans(n_clusters=k, random_state=42)
df['kmeans'] = kmeans.fit_predict(df[['latitude', 'longitude']])
cluster_order = sorted(df['kmeans'].unique())  # 클러스터 순서 정렬

# 중심 좌표 설정 (예: 위도/경도 평균)
center_lat = df['latitude'].mean()
center_lon = df['longitude'].mean()

# 클러스터 색상 팔레트 정의
palette_rgb = sns.color_palette("tab10", n_colors=k)
palette = {i: mcolors.to_hex(rgb) for i, rgb in enumerate(palette_rgb)}

# folium 지도 생성
m = folium.Map(location=[center_lat, center_lon], zoom_start=11)

# 클러스터별 마커 추가
for _, row in df.iterrows():
    cluster = row['kmeans']
    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=4,
        color=palette[cluster],
        fill=True,
        fill_opacity=0.7,
        popup=f"Cluster {cluster}"
    ).add_to(m)

# 지도 HTML로 저장
m.save("kmeans_cluster_map.html")