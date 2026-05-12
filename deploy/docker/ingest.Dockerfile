FROM driver-analytics-base:latest
CMD ["python", "-m", "packages.ingest.rtsp_worker"]
