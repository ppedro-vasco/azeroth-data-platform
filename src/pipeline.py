from dagster import Definitions, asset

@asset
def test_setup():
    return "INFRA STATUS OK!"
    

assets = [test_setup]

defs = Definitions(
    assets = assets
)