FROM driver-analytics-base:latest
CMD ["python", "-m", "packages.inference.face_worker"]
