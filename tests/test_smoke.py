def test_import_flakynet() -> None:
    import flakynet

    assert flakynet.__name__ == "flakynet"
