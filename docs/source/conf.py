# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.

import os
import sys

from pygments.lexers.python import PythonLexer
from sphinx.highlighting import lexers

sys.path.insert(0, os.path.abspath(os.path.join("..", "..", "src")))

lexers["ipython3"] = PythonLexer()


# -- Project information -----------------------------------------------------

project = "OpenGRIS Scaler"
author = "Citi"

with open("../../src/scaler/version.txt", "rt") as f:
    version = f.read().strip()

release = f"{version}-py3-none-any"

rst_prolog = f"""
.. |version| replace:: {version}
.. |release| replace:: {release}
"""

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx_substitution_extensions",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosectionlabel",
    "sphinx_copybutton",
    "sphinx_tabs.tabs",
    "nbsphinx",
    "jupyterlite_sphinx",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
# ``debug_*.ipynb`` are local debug notebooks (e.g. for the wasm/JupyterLite
# harness in scripts/test_jupyterlite.sh) -- they are still served by JupyterLite via
# ``jupyterlite_contents`` below but should not appear in the published docs.
exclude_patterns = ["gallery/debug_*.ipynb", "gallery/*_scaler_only.ipynb"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
# html_theme = "alabaster"

html_theme = "shibuya"
html_title = f"{project} {version}"

html_theme_options = {
    "nav_links": [
        {"title": "Release Notes", "url": "release_notes"},
        {"title": "Example Gallery", "url": "gallery/index"},
        {"title": "Launchpad", "url": "launchpad/", "resource": True},
    ]
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_extra_path = ["../html_extra"]
html_css_files = ["style.css"]

# html_static_path = []
# html_css_files = []


# -- Extension configuration -------------------------------------------------

autosectionlabel_prefix_document = True

copybutton_prompt_text = r"\$ "
copybutton_prompt_is_regexp = True

nbsphinx_execute = "never"
nbsphinx_codecell_lexer = "python"

# -- JupyterLite (Try in your browser) --------------------------------------
# jupyterlite-sphinx builds a JupyterLite (Pyodide) site under build/html/lite
# during ``make html`` and exposes the listed notebooks inside it.
jupyterlite_contents = [
    "gallery/parallel_sqrt.ipynb",
    "gallery/send_heavy_object.ipynb",
    "gallery/AlphaResearch.ipynb",
    "gallery/AlphaResearch_scaler_only.ipynb",
    "gallery/VolSurface.ipynb",
    "gallery/VolSurface_scaler_only.ipynb",
    "gallery/SwapCVA.ipynb",
    "gallery/SwapCVA_scaler_only.ipynb",
    "gallery/XVA.ipynb",
    "gallery/XVA_scaler_only.ipynb",
]

# Bundle the scaler wasm wheel + cloudpickle + tblib into the lite kernel's
# pypi index so ``await piplite.install("opengris-scaler")`` resolves to local
# URLs (no network needed). The config file is regenerated from the wheels in
# ``_static/wasm/`` on every doc build (see ``_regen_jupyterlite_config`` below)
# so its contents track the actual versioned wheel filenames automatically and
# the config does not need to be checked in.
jupyterlite_config = "jupyter_lite_config.json"


def _regen_jupyterlite_config():
    """Regenerate ``jupyter_lite_config.json`` from the wheels in ``_static/wasm``.

    Runs at conf.py import time, before jupyterlite-sphinx reads the config.
    The file is gitignored; the wheel filenames are versioned (e.g.
    ``opengris_scaler-2.3.0-cp313-cp313-emscripten_4_0_9_wasm32.whl``) so the
    config has to be derived from whatever is on disk at build time.
    """
    import sys as _sys
    from pathlib import Path as _Path

    _scripts = _Path(__file__).resolve().parent.parent.parent / "scripts"
    _sys.path.insert(0, str(_scripts))
    try:
        import generate_jupyterlite_config as _gen

        _gen.main()
    except SystemExit as exc:
        # Wheels not staged yet (e.g. running sphinx without a prior wasm
        # build). Leave any stale config in place and let jupyterlite-sphinx
        # surface the real error.
        print(f"[conf.py] skipping jupyter_lite_config regen: {exc}")
    finally:
        _sys.path.pop(0)


_regen_jupyterlite_config()

# Inject a styled "Try in your browser" banner at the top of every rendered
# notebook so users landing directly on a notebook page see the option.
nbsphinx_prolog = r"""
{% set notebook = env.doc2path(env.docname, base=None).split('/')[-1] %}

.. raw:: html

    <div class="try-in-browser-banner">
      <a class="try-in-browser"
         href="../lite/lab/index.html?path={{ notebook }}"
         target="_blank"
         rel="noopener">
        ▶ Try this notebook in your browser (no install)
      </a>
    </div>
"""


# -- Auto-install opengris-scaler in the JupyterLite kernel ------------------
# After ``jupyter lite build`` finishes (which jupyterlite-sphinx triggers
# during ``make html``), patch the kernel boot bundle so ``import scaler``
# Just Works in browser notebooks without a visible install cell. See
# scripts/patch_jupyterlite_kernel.py for the full rationale.
def _patch_jupyterlite_kernel(app, exception):
    if exception is not None:
        return
    if app.builder.name != "html":
        return

    import sys as _sys
    from pathlib import Path as _Path

    repo_root = _Path(app.srcdir).resolve().parent.parent
    lite_dir = _Path(app.outdir) / "lite"
    if not lite_dir.is_dir():
        return

    _sys.path.insert(0, str(repo_root / "scripts"))
    try:
        import patch_jupyterlite_kernel as _patcher
    finally:
        _sys.path.pop(0)
    _patcher.patch_tree(lite_dir)


def setup(app):
    app.connect("build-finished", _patch_jupyterlite_kernel)
