import pytest


@pytest.fixture(scope='session', autouse=True)
def global_init():
    print("\n======= Global initialization =======")
    yield
    print("\n======= Global cleanup =======")
