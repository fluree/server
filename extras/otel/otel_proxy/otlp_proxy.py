import os
import json
import yaml
from flask import Flask, request
import requests
import sys
from google.protobuf.json_format import MessageToJson
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest

app = Flask(__name__)

PORT = os.getenv("PORT", "4318").strip()
FORWARD_TRACES_URL = os.getenv("FORWARD_TRACES_URL", "http://otel-collector:4318").strip()
FORWARD_METRICS_URL = os.getenv("FORWARD_METRICS_URL", "http://otel-collector:4318").strip()
FORWARD_LOGS_URL = os.getenv("FORWARD_LOGS_URL", "http://otel-collector:4318").strip()

@app.route('/v1/traces', methods=['POST'])
def handle_traces():
    return process_otlp_request(request, ExportTraceServiceRequest, "/v1/traces", FORWARD_TRACES_URL)

@app.route('/v1/metrics', methods=['POST'])
def handle_metrics():
    return process_otlp_request(request, ExportMetricsServiceRequest, "/v1/metrics", FORWARD_METRICS_URL)

@app.route('/v1/logs', methods=['POST'])
def handle_logs():
    return process_otlp_request(request, ExportLogsServiceRequest, "/v1/logs", FORWARD_LOGS_URL)

def process_otlp_request(req, proto_class, path, forward_url):
    try:
        # Decode Protocol Buffers (OTLP data)
        otlp_message = proto_class()
        otlp_message.ParseFromString(req.data)
        json_data = MessageToJson(otlp_message)

        parsed_json = json.loads(json_data)

        yaml.dump(parsed_json, stream=sys.stdout, default_flow_style=False)
        sys.stdout.flush()

        # Forward data only if a forwarding URL is set
        if forward_url:
            response = requests.post(forward_url + path, data=req.data, headers=req.headers)
            return response.text, response.status_code
        else:
            return "", 200  # Return HTTP 200 OK with empty body when not forwarding

    except Exception as e:
        print(f"Error processing OTLP request: {e}")
        return "Error processing data", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, debug=True)
