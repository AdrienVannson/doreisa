import pytest
import subprocess


@pytest.fixture
def ray_cluster():
    """Start a Ray cluster for this test"""
    subprocess.run(["ray", "start", "--head"], check=True)

    yield

    subprocess.run(["ray", "stop"], check=True)