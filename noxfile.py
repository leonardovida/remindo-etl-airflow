# noxfile.py
import nox
import tempfile

locations = "src", "noxfile.py"
# Exclude modifying files with black on runtime
nox.options.sessions = (
    "lint",
    "tests",
    "safety",
)


@nox.session(python=["3.8", "3.7"])
def lint(session):
    args = session.posargs or locations
    session.install(
        "flake8",
        "flake8-black",
        "flake8-import-order",
        "flake8-bandit",
    )
    session.run("flake8", *args)


@nox.session(python="3.8")
def black(session):
    """Automatically lint code with black"""
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