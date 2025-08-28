"""Centralized configuration with layered settings.

This module loads environment variables using ``python-dotenv`` and exposes
grouped configuration objects for Google Cloud Platform (GCP) and VALD API
settings.  Values are organised in two layers:

``Settings`` -> ``GCPConfig`` and ``VALDAPIConfig``

Each processing script can import :data:`settings` and access nested
attributes, e.g. ``settings.gcp.project_id`` or ``settings.vald_api.tenant_id``.
"""

from __future__ import annotations

from dataclasses import dataclass
import os

from dotenv import load_dotenv

# Load environment variables from a ``.env`` file if present. Values in the
# actual environment take precedence over those defined in the file.
load_dotenv()


@dataclass
class GCPConfig:
    """Configuration options for Google Cloud resources."""

    project_id: str | None = os.getenv("GCP_PROJECT_ID")
    dataset_id: str | None = os.getenv("GCP_DATASET_ID")
    credentials_file: str = os.getenv("GCP_CREDENTIALS_FILE", "gcp_credentials.json")
    cmj_table_id: str = os.getenv("CMJ_TABLE_ID", "cmj_results")
    hj_table_id: str = os.getenv("HJ_TABLE_ID", "hj_results")
    imtp_table_id: str = os.getenv("IMTP_TABLE_ID", "imtp_results")
    ppu_table_id: str = os.getenv("PPU_TABLE_ID", "ppu_results")


@dataclass
class VALDAPIConfig:
    """Configuration options for VALD API endpoints and credentials."""

    forcedecks_url: str | None = os.getenv("FORCEDECKS_URL")
    dynamo_url: str | None = os.getenv("DYNAMO_URL")
    profile_url: str | None = os.getenv("PROFILE_URL")
    tenant_id: str | None = os.getenv("TENANT_ID")
    client_id: str | None = os.getenv("CLIENT_ID")
    client_secret: str | None = os.getenv("CLIENT_SECRET")
    auth_url: str | None = os.getenv("AUTH_URL")


@dataclass
class Settings:
    """Top-level application settings grouping all configuration layers."""

    gcp: GCPConfig = GCPConfig()
    vald_api: VALDAPIConfig = VALDAPIConfig()


# Single instance used by the rest of the application
settings = Settings()

__all__ = ["settings", "GCPConfig", "VALDAPIConfig", "Settings"]