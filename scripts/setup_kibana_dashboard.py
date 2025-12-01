import requests
import json

KIBANA_URL = "http://localhost:5601"
HEADERS = {
    "kbn-xsrf": "true",
    "Content-Type": "application/json"
}

### 1. Create Data View ########################################################
data_view = {
  "attributes": {
    "title": "mental-health-index",
    "timeFieldName": "timestamp",
    "name": "mental-health-view"
  }
}

resp = requests.post(
    f"{KIBANA_URL}/api/data_views/data_view",
    headers=HEADERS,
    data=json.dumps(data_view)
)
print("Data View Response:", resp.status_code, resp.text)


### 2. Create Visualizations ####################################################

# A. Messages Over Time (Line Chart)
vis_messages_over_time = {
  "attributes": {
    "title": "Messages Over Time",
    "visState": json.dumps({
        "type": "line",
        "data": {"aggs": []},
        "params": {
            "seriesParams": [{"type": "line", "data": {"id": "1"}}]
        }
    }),
    "kibanaSavedObjectMeta": {
      "searchSourceJSON": json.dumps({
        "query": {"query": "", "language": "kuery"},
        "filter": [],
        "index": "mental-health-index"
      })
    }
  }
}

resp = requests.post(
    f"{KIBANA_URL}/api/saved_objects/visualization",
    headers=HEADERS,
    data=json.dumps(vis_messages_over_time)
)
viz1 = resp.json()
print("Vis 1:", resp.status_code)

# B. Sentiment Distribution (Bar Chart)
vis_sentiment = {
  "attributes": {
    "title": "Sentiment Distribution",
    "visState": json.dumps({
        "type": "histogram",
        "data": {"aggs": []},
        "params": {}
    }),
    "kibanaSavedObjectMeta": {
      "searchSourceJSON": json.dumps({
        "query": {"query": "", "language": "kuery"},
        "filter": [],
        "index": "mental-health-index"
      })
    }
  }
}

resp = requests.post(
    f"{KIBANA_URL}/api/saved_objects/visualization",
    headers=HEADERS,
    data=json.dumps(vis_sentiment)
)
viz2 = resp.json()
print("Vis 2:", resp.status_code)

# C. All Messages Table
vis_table = {
  "attributes": {
    "title": "All Messages",
    "visState": json.dumps({
        "type": "table",
        "params": {"showMetricsAtAllLevels": True}
    }),
    "kibanaSavedObjectMeta": {
      "searchSourceJSON": json.dumps({
        "query": {"query": "", "language": "kuery"},
        "filter": [],
        "index": "mental-health-index"
      })
    }
  }
}

resp = requests.post(
    f"{KIBANA_URL}/api/saved_objects/visualization",
    headers=HEADERS,
    data=json.dumps(vis_table)
)
viz3 = resp.json()
print("Vis 3:", resp.status_code)

### 3. Create Dashboard #########################################################
dashboard = {
  "attributes": {
    "title": "Mental Health Dashboard",
    "panelsJSON": json.dumps([
        {
            "panelIndex": "1",
            "type": "visualization",
            "id": viz1["id"],
            "version": "8.13.0",
            "gridData": {"x": 0, "y": 0, "w": 24, "h": 15, "i": "1"}
        },
        {
            "panelIndex": "2",
            "type": "visualization",
            "id": viz2["id"],
            "version": "8.13.0",
            "gridData": {"x": 0, "y": 16, "w": 12, "h": 12, "i": "2"}
        },
        {
            "panelIndex": "3",
            "type": "visualization",
            "id": viz3["id"],
            "version":  "8.13.0",
            "gridData": {"x": 13, "y": 16, "w": 12, "h": 12, "i": "3"}
        }
    ])
  }
}

resp = requests.post(
    f"{KIBANA_URL}/api/saved_objects/dashboard",
    headers=HEADERS,
    data=json.dumps(dashboard)
)

print("Dashboard:", resp.status_code, resp.text)

print("\n🎉 DONE! Dashboard created at:")
print("👉 http://localhost:5601/app/dashboards")
