def test_import_main_module() -> None:
    """Basic smoke test to ensure main module imports."""
    import app.main  # noqa: F401

