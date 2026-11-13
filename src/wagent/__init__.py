"""Reliable WhatsApp delivery for agents (under development)."""

from importlib.metadata import version

from wagent.app import Wagent
from wagent.settings import Settings

__version__ = version("wagent")
__all__ = ["Settings", "Wagent", "__version__"]
