from dagster import Definitions, load_assets_from_modules

from data_pipeline.assets.bronze import ingestion
from data_pipeline.assets.silver import transformation, dimensions

bronze_assets = load_assets_from_modules([ingestion])
silver_assets = load_assets_from_modules([transformation, dimensions])

defs = Definitions(
    assets=[*bronze_assets, *silver_assets],
)