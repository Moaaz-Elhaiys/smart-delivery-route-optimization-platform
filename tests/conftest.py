# tests/conftest.py
import pytest
from ingestion.data_simulator import simulate_orders, simulate_drivers

@pytest.fixture
def sample_orders():
    return simulate_orders(n=10, seed=42)

@pytest.fixture
def sample_drivers():
    return simulate_drivers(n=3, seed=42)

@pytest.fixture
def sample_locations():
    return [(30.05, 31.25), (30.06, 31.22), (30.04, 31.28)]