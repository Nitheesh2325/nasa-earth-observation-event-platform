"""FastAPI-only Version 1.0 Streamlit dashboard."""

from .client import DashboardApiClient, DashboardApiError

__all__ = ["DashboardApiClient", "DashboardApiError"]
