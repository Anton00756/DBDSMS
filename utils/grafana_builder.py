import json
import uuid
from datetime import datetime


class GrafanaBuilder:
    def __init__(self):
        self.data = {
            "__inputs": [
                {
                    "name": "DS_PROMETHEUS",
                    "label": "Prometheus",
                    "description": "",
                    "type": "datasource",
                    "pluginId": "prometheus",
                    "pluginName": "Prometheus"
                }
            ],
            "__elements": {},
            "__requires": [
                {
                    "type": "grafana",
                    "id": "grafana",
                    "name": "Grafana",
                    "version": "10.1.2"
                },
                {
                    "type": "datasource",
                    "id": "prometheus",
                    "name": "Prometheus",
                    "version": "1.0.0"
                },
                {
                    "type": "panel",
                    "id": "timeseries",
                    "name": "Time series",
                    "version": ""
                }
            ],
            "annotations": {
                "list": [
                    {
                        "builtIn": 1,
                        "datasource": {
                            "type": "grafana",
                            "uid": "-- Grafana --"
                        },
                        "enable": True,
                        "hide": True,
                        "iconColor": "rgba(0, 211, 255, 1)",
                        "name": "Annotations & Alerts",
                        "type": "dashboard"
                    }
                ]
            },
            "editable": True,
            "fiscalYearStartMonth": 0,
            "graphTooltip": 0,
            "id": None,
            "links": [],
            "liveNow": False,
            "panels": [],
            "refresh": "5s",
            "schemaVersion": 38,
            "style": "dark",
            "tags": [],
            "templating": {
                "list": []
            },
            "time": {
                "from": "now-15m",
                "to": "now"
            },
            "timepicker": {},
            "timezone": "",
            "title": f"DBDSMS [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]",
            "uid": str(uuid.uuid4()),
            "version": 8,
            "weekStart": ""
        }
        self.id = 0

    def add_filter(self, stream: str, index: int):
        self.data['panels'].append({
            "datasource": {
                "type": "prometheus",
                "uid": "${DS_PROMETHEUS}"
            },
            "description": "",
            "fieldConfig": {
                "defaults": {
                    "color": {
                        "mode": "palette-classic"
                    },
                    "custom": {
                        "axisCenteredZero": False,
                        "axisColorMode": "text",
                        "axisLabel": "",
                        "axisPlacement": "auto",
                        "barAlignment": 0,
                        "drawStyle": "line",
                        "fillOpacity": 0,
                        "gradientMode": "none",
                        "hideFrom": {
                            "legend": False,
                            "tooltip": False,
                            "viz": False
                        },
                        "insertNulls": False,
                        "lineInterpolation": "linear",
                        "lineWidth": 1,
                        "pointSize": 5,
                        "scaleDistribution": {
                            "type": "linear"
                        },
                        "showPoints": "auto",
                        "spanNulls": False,
                        "stacking": {
                            "group": "A",
                            "mode": "none"
                        },
                        "thresholdsStyle": {
                            "mode": "off"
                        }
                    },
                    "mappings": [],
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {
                                "color": "green",
                                "value": None
                            },
                            {
                                "color": "red",
                                "value": 80
                            }
                        ]
                    }
                },
                "overrides": []
            },
            "gridPos": {
                "h": 8,
                "w": 12,
                "x": (self.id % 2) * 12,
                "y": (self.id // 2) * 8
            },
            "id": self.id,
            "options": {
                "legend": {
                    "calcs": [],
                    "displayMode": "list",
                    "placement": "bottom",
                    "showLegend": True
                },
                "tooltip": {
                    "mode": "single",
                    "sort": "none"
                }
            },
            "targets": [
                {
                    "datasource": {
                        "type": "prometheus",
                        "uid": "${DS_PROMETHEUS}"
                    },
                    "disableTextWrap": False,
                    "editorMode": "builder",
                    "expr": f"flink_taskmanager_job_task_operator__{stream}:{index}__filter_in",
                    "fullMetaSearch": False,
                    "includeNullMetadata": True,
                    "instant": False,
                    "legendFormat": "\u0412\u0445\u043e\u0434",
                    "range": True,
                    "refId": "A",
                    "useBackend": False
                },
                {
                    "datasource": {
                        "type": "prometheus",
                        "uid": "${DS_PROMETHEUS}"
                    },
                    "disableTextWrap": False,
                    "editorMode": "builder",
                    "expr": f"flink_taskmanager_job_task_operator__{stream}:{index}__filter_out",
                    "fullMetaSearch": False,
                    "hide": False,
                    "includeNullMetadata": True,
                    "instant": False,
                    "legendFormat": "\u0412\u044b\u0445\u043e\u0434",
                    "range": True,
                    "refId": "B",
                    "useBackend": False
                }
            ],
            "title": f"\"{stream}\": {index} [Filter]",
            "type": "timeseries"
        })
        self.id += 1

    def add_deduplicator(self, stream: str, index: int):
        self.data['panels'].append({
            "datasource": {
                "type": "prometheus",
                "uid": "${DS_PROMETHEUS}"
            },
            "description": "",
            "fieldConfig": {
                "defaults": {
                    "color": {
                        "mode": "palette-classic"
                    },
                    "custom": {
                        "axisCenteredZero": False,
                        "axisColorMode": "text",
                        "axisLabel": "",
                        "axisPlacement": "auto",
                        "barAlignment": 0,
                        "drawStyle": "line",
                        "fillOpacity": 0,
                        "gradientMode": "none",
                        "hideFrom": {
                            "legend": False,
                            "tooltip": False,
                            "viz": False
                        },
                        "insertNulls": False,
                        "lineInterpolation": "linear",
                        "lineWidth": 1,
                        "pointSize": 5,
                        "scaleDistribution": {
                            "type": "linear"
                        },
                        "showPoints": "auto",
                        "spanNulls": False,
                        "stacking": {
                            "group": "A",
                            "mode": "none"
                        },
                        "thresholdsStyle": {
                            "mode": "off"
                        }
                    },
                    "mappings": [],
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {
                                "color": "green",
                                "value": None
                            },
                            {
                                "color": "red",
                                "value": 80
                            }
                        ]
                    }
                },
                "overrides": []
            },
            "gridPos": {
                "h": 8,
                "w": 12,
                "x": (self.id % 2) * 12,
                "y": (self.id // 2) * 8
            },
            "id": self.id,
            "options": {
                "legend": {
                    "calcs": [],
                    "displayMode": "list",
                    "placement": "bottom",
                    "showLegend": True
                },
                "tooltip": {
                    "mode": "single",
                    "sort": "none"
                }
            },
            "targets": [
                {
                    "datasource": {
                        "type": "prometheus",
                        "uid": "${DS_PROMETHEUS}"
                    },
                    "disableTextWrap": False,
                    "editorMode": "builder",
                    "exemplar": False,
                    "expr": f"flink_taskmanager_job_task_operator__{stream}:{index}__deduplicator_in",
                    "fullMetaSearch": False,
                    "includeNullMetadata": True,
                    "instant": False,
                    "legendFormat": "\u0412\u0445\u043e\u0434",
                    "range": True,
                    "refId": "A",
                    "useBackend": False
                },
                {
                    "datasource": {
                        "type": "prometheus",
                        "uid": "${DS_PROMETHEUS}"
                    },
                    "disableTextWrap": False,
                    "editorMode": "builder",
                    "expr": f"flink_taskmanager_job_task_operator__{stream}:{index}__deduplicator_out",
                    "fullMetaSearch": False,
                    "hide": False,
                    "includeNullMetadata": True,
                    "instant": False,
                    "legendFormat": "\u0412\u044b\u0445\u043e\u0434",
                    "range": True,
                    "refId": "B",
                    "useBackend": False
                }
            ],
            "title": f"\"{stream}\": {index} [Deduplicator]",
            "type": "timeseries"
        })
        self.id += 1

    def add_enricher(self, stream: str, index: int):
        self.data['panels'].append({
            "datasource": {
                "type": "prometheus",
                "uid": "${DS_PROMETHEUS}"
            },
            "description": "",
            "fieldConfig": {
                "defaults": {
                    "color": {
                        "mode": "palette-classic"
                    },
                    "custom": {
                        "axisCenteredZero": False,
                        "axisColorMode": "text",
                        "axisLabel": "",
                        "axisPlacement": "auto",
                        "barAlignment": 0,
                        "drawStyle": "line",
                        "fillOpacity": 0,
                        "gradientMode": "none",
                        "hideFrom": {
                            "legend": False,
                            "tooltip": False,
                            "viz": False
                        },
                        "insertNulls": False,
                        "lineInterpolation": "linear",
                        "lineWidth": 1,
                        "pointSize": 5,
                        "scaleDistribution": {
                            "type": "linear"
                        },
                        "showPoints": "auto",
                        "spanNulls": False,
                        "stacking": {
                            "group": "A",
                            "mode": "none"
                        },
                        "thresholdsStyle": {
                            "mode": "off"
                        }
                    },
                    "mappings": [],
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {
                                "color": "green",
                                "value": None
                            },
                            {
                                "color": "red",
                                "value": 80
                            }
                        ]
                    }
                },
                "overrides": []
            },
            "gridPos": {
                "h": 8,
                "w": 12,
                "x": (self.id % 2) * 12,
                "y": (self.id // 2) * 8
            },
            "id": self.id,
            "options": {
                "legend": {
                    "calcs": [],
                    "displayMode": "list",
                    "placement": "bottom",
                    "showLegend": True
                },
                "tooltip": {
                    "mode": "single",
                    "sort": "none"
                }
            },
            "targets": [
                {
                    "datasource": {
                        "type": "prometheus",
                        "uid": "${DS_PROMETHEUS}"
                    },
                    "disableTextWrap": False,
                    "editorMode": "builder",
                    "expr": f"flink_taskmanager_job_task_operator__{stream}:{index}__enricher_in",
                    "fullMetaSearch": False,
                    "includeNullMetadata": True,
                    "instant": False,
                    "legendFormat": "\u0412\u0445\u043e\u0434",
                    "range": True,
                    "refId": "A",
                    "useBackend": False
                },
                {
                    "datasource": {
                        "type": "prometheus",
                        "uid": "${DS_PROMETHEUS}"
                    },
                    "disableTextWrap": False,
                    "editorMode": "builder",
                    "expr": f"flink_taskmanager_job_task_operator__{stream}:{index}__enrichment_count",
                    "fullMetaSearch": False,
                    "hide": False,
                    "includeNullMetadata": True,
                    "instant": False,
                    "legendFormat": "\u041a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e \u043e\u0431\u043e"
                                    "\u0433\u0430\u0449\u0435\u043d\u0438\u0439",
                    "range": True,
                    "refId": "B",
                    "useBackend": False
                }
            ],
            "title": f"\"{stream}\": {index} [Enricher]",
            "type": "timeseries"
        })
        self.id += 1

    def add_stream_joiner(self, stream: str, index: int, second_stream: str):
        self.data['panels'].append({
            "datasource": {
                "type": "prometheus",
                "uid": "${DS_PROMETHEUS}"
            },
            "description": "",
            "fieldConfig": {
                "defaults": {
                    "color": {
                        "mode": "palette-classic"
                    },
                    "custom": {
                        "axisCenteredZero": False,
                        "axisColorMode": "text",
                        "axisLabel": "",
                        "axisPlacement": "auto",
                        "barAlignment": 0,
                        "drawStyle": "line",
                        "fillOpacity": 0,
                        "gradientMode": "none",
                        "hideFrom": {
                            "legend": False,
                            "tooltip": False,
                            "viz": False
                        },
                        "insertNulls": False,
                        "lineInterpolation": "linear",
                        "lineWidth": 1,
                        "pointSize": 5,
                        "scaleDistribution": {
                            "type": "linear"
                        },
                        "showPoints": "auto",
                        "spanNulls": False,
                        "stacking": {
                            "group": "A",
                            "mode": "none"
                        },
                        "thresholdsStyle": {
                            "mode": "off"
                        }
                    },
                    "mappings": [],
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {
                                "color": "green",
                                "value": None
                            },
                            {
                                "color": "red",
                                "value": 80
                            }
                        ]
                    }
                },
                "overrides": []
            },
            "gridPos": {
                "h": 8,
                "w": 12,
                "x": (self.id % 2) * 12,
                "y": (self.id // 2) * 8
            },
            "id": self.id,
            "options": {
                "legend": {
                    "calcs": [],
                    "displayMode": "list",
                    "placement": "bottom",
                    "showLegend": True
                },
                "tooltip": {
                    "mode": "single",
                    "sort": "none"
                }
            },
            "targets": [
                {
                    "datasource": {
                        "type": "prometheus",
                        "uid": "${DS_PROMETHEUS}"
                    },
                    "disableTextWrap": False,
                    "editorMode": "builder",
                    "expr": f"flink_taskmanager_job_task_operator__{stream}:{index}__stream_joiner_first_in",
                    "fullMetaSearch": False,
                    "includeNullMetadata": True,
                    "instant": False,
                    "legendFormat": "\u041e\u0441\u043d\u043e\u0432\u043d\u043e\u0439 \u0432\u0445\u043e\u0434",
                    "range": True,
                    "refId": "A",
                    "useBackend": False
                },
                {
                    "datasource": {
                        "type": "prometheus",
                        "uid": "${DS_PROMETHEUS}"
                    },
                    "disableTextWrap": False,
                    "editorMode": "builder",
                    "expr": f"flink_taskmanager_job_task_operator__{stream}:{index}__stream_joiner_second_in",
                    "fullMetaSearch": False,
                    "hide": False,
                    "includeNullMetadata": True,
                    "instant": False,
                    "legendFormat": f"\u0412\u0442\u043e\u0440\u043e\u0441\u0442\u0435\u043f\u0435\u043d\u043d\u044b"
                                    f"\u0439 \u0432\u0445\u043e\u0434 (\"{second_stream}\")",
                    "range": True,
                    "refId": "B",
                    "useBackend": False
                },
                {
                    "datasource": {
                        "type": "prometheus",
                        "uid": "${DS_PROMETHEUS}"
                    },
                    "disableTextWrap": False,
                    "editorMode": "builder",
                    "expr": f"flink_taskmanager_job_task_operator__{stream}:{index}__stream_joiner_out",
                    "fullMetaSearch": False,
                    "hide": False,
                    "includeNullMetadata": True,
                    "instant": False,
                    "legendFormat": "\u0412\u044b\u0445\u043e\u0434",
                    "range": True,
                    "refId": "C",
                    "useBackend": False
                }
            ],
            "title": f"\"{stream}\": {index} [StreamJoiner]",
            "type": "timeseries"
        })
        self.id += 1

    def save(self, path: str):
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(self.data, file, indent=4)
