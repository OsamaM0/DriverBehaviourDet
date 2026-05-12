FROM driver-analytics-base:latest
CMD ["python", "-m", "packages.inference.detector_worker"]
