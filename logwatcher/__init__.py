from importlib.metadata import version, PackageNotFoundError

try:
    # Use the name exactly as it appears in the [project] section of pyproject.toml
    __version__ = version("dynostore-logwatcher")
except PackageNotFoundError:
    # Package is not installed (e.g., running directly from source tree without installing)
    __version__ = "unknown"