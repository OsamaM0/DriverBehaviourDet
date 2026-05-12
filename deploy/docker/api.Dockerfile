FROM driver-analytics-base:latest
EXPOSE 8080
CMD ["uvicorn", "packages.api.main:app", "--host", "0.0.0.0", "--port", "8080"]
