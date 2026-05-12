FROM driver-analytics-base:latest
CMD ["python", "-m", "packages.alerts.alert_service"]
