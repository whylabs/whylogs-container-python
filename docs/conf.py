import os
import sys

sys.path.insert(0, os.path.abspath("../"))


project = "whylogs_container"
copyright = "2024, anthony@whylabs.ai, murilo@whylabs.ai"
author = "anthony@whylabs.ai, murilo@whylabs.ai"
release = "3.0.0"

templates_path = []
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
]

# Autodoc settings
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}

html_theme = "furo"
html_static_path = ["_static"]

autosummary_generate = True
