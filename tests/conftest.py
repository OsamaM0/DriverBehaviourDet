import os
import sys
from pathlib import Path

# Make the workspace root importable so `import packages.*` works
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Sane test-only defaults so Settings() never blocks on missing infra
os.environ.setdefault("KAFKA_BOOTSTRAP", "localhost:9092")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/driver_analytics")
os.environ.setdefault("S3_ENDPOINT", "http://localhost:9000")
os.environ.setdefault("TRITON_URL", "localhost:8001")
