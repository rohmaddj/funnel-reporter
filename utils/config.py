"""
Config loader — reads config.yaml once and caches it.
All collectors import from here instead of hardcoding mappings.
"""

import os
from pathlib import Path

_config = None

def load_config() -> dict:
    global _config
    if _config is not None:
        return _config

    config_path = Path(os.environ.get("CONFIG_FILE", "config.yaml"))
    if not config_path.exists():
        raise FileNotFoundError(
            f"config.yaml not found at {config_path.resolve()}\n"
            f"  Make sure config.yaml is in the project root."
        )

    import yaml
    with open(config_path) as f:
        _config = yaml.safe_load(f)
    return _config


def get_sku_map() -> dict:
    """Returns {sku: {label, price, stage}} for ClickBank processing."""
    return load_config()["clickbank"]["skus"]


def get_variant_map() -> dict:
    """Returns {cpv_campaign_id: variant_key} for ClickBank attribution."""
    campaigns = load_config()["cpvlabs"]["campaigns"]
    return {cid: data["variant"] for cid, data in campaigns.items()}


def get_campaign_map() -> dict:
    """Returns full CPV campaign config {id: {label, source, variant}}."""
    return load_config()["cpvlabs"]["campaigns"]


def get_variants() -> dict:
    """Returns variant definitions {key: {label, path, exclude}}."""
    return load_config()["variants"]


def get_cpv_group() -> str:
    return load_config()["cpvlabs"]["group"]