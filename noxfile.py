# noxfile.py
import tempfile
from typing import Any

import nox
from nox.sessions import Session

locations = "src", "noxfile.py"
# Exclude modifying files with black on runtime
nox.options.sessions = (
    "lint",
    "tests",
    "safety",
    "docs/conf.py",
)


def install_with_constraints(session: Session, *args: str, **kwargs: Any) -> None:
    """Install packages constrained by Poetry's lock file.
    This function is a wrapper for nox.sessions.Session.install. It
    invokes pip to install packages inside of the session's virtualenv.
    Additionally, pip is passed a constraints file generated from
    Poetry's lock file, to ensure that the packages are pinned to the
    versions specified in poetry.lock. This allows you to manage the
    packages as Poetry development dependencies.
    Arguments:
        session: The Session object.
        args: Command-line arguments for pip.
        kwargs: Additional keyword arguments for Session.install.
    """
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "pip",
            "freeze",
            ">",
            "requirements.txt",
        )
        session.install(f"--constraint={requirements.name}", *args, **kwargs)


@nox.session(python=["3.8", "3.7"])
def lint(session):
    """Lint using flake8."""
    args = session.posargs or locations
    session.install_with_constraints(
        "flake8",
        "flake8-black",
        "flake8-import-order",
        "flake8-bandit",
        "flake8-docstrings",
    )
    session.run("flake8", *args)


@nox.session(python="3.8")
def black(session):
    """Automatically run black code formatter"""
    args = session.posargs or locations
    session.install("black")
    session.run("black", *args)


@nox.session(python="3.8")
def safety(session):
    """Find security problems in dependencies.

    Uses pip instead of poetry."""
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "pip",
            "freeze",
            ">",
            "requirements.txt",
        )
        session.install("safety")
        session.run("safety", "check", f"--file={requirements.name}", "--full-report")


@nox.session(python="3.8")
def docs(session: Session) -> None:
    """Build the documentation."""
    install_with_constraints(session, "sphinx", "sphinx-autodoc-typehints")
    session.run("sphinx-build", "docs", "docs/_build")