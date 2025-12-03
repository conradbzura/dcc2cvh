"""DCC (Data Coordinating Center) configuration and utilities for CFDE metadata aggregator."""

DCC_CONFIG = {
    "4dn": {
        "name": "4DN",
        "display_name": "4D NUCLEOME DATA COORDINATION AND INTEGRATION CENTER",
        "s3_base": "https://cfde-drc.s3.amazonaws.com/4DN/C2M2/",
        "latest_url": "https://cfde-drc.s3.amazonaws.com/4DN/C2M2/2025-09-24/250924_c2m2_4dn_sub.zip",
    },
    "hubmap": {
        "name": "HuBMAP",
        "display_name": "Human BioMolecular Atlas Program",
        "s3_base": "https://cfde-drc.s3.amazonaws.com/HuBMAP/C2M2/",
        "latest_url": "https://cfde-drc.s3.amazonaws.com/HuBMAP/C2M2/2025-09-15/HuBMAP_C2M2_Fall_2025.zip",
    },
}


def normalize_dcc_name(name: str) -> str:
    """
    Normalize DCC name to lowercase for comparison.

    Args:
        name: DCC name (case-insensitive)

    Returns:
        Normalized DCC name in lowercase
    """
    return name.lower().strip()


def get_dcc_config(dcc: str) -> dict:
    """
    Get configuration for a specific DCC.

    Args:
        dcc: DCC name (case-insensitive)

    Returns:
        Dictionary containing DCC configuration

    Raises:
        KeyError: If DCC name is not recognized
    """
    normalized = normalize_dcc_name(dcc)
    if normalized not in DCC_CONFIG:
        raise KeyError(
            f"Unknown DCC '{dcc}'. Available DCCs: {', '.join(get_all_dcc_names())}"
        )
    return DCC_CONFIG[normalized]


def get_all_dcc_names() -> list[str]:
    """
    Get list of all supported DCC names.

    Returns:
        List of supported DCC names in lowercase
    """
    return sorted(DCC_CONFIG.keys())


def get_dcc_display_name(dcc: str) -> str:
    """
    Get display name for a DCC.

    Args:
        dcc: DCC name (case-insensitive)

    Returns:
        Display name of the DCC
    """
    config = get_dcc_config(dcc)
    return config["display_name"]
