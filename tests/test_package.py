def test_package_imports():
    import flakynet

    assert flakynet.__name__ == "flakynet"


def test_public_exports():
    import flakynet as fn

    assert fn.Plan
    assert fn.Latency
    assert fn.Bandwidth
    assert fn.Fragment
    assert fn.Reset
    assert fn.CloseAfter
    assert fn.Blackhole
    assert fn.RefuseConnect
    assert callable(fn.between)
    assert callable(fn.nth)
    assert callable(fn.every)
    assert callable(fn.saw)
    assert fn.ALL
    assert callable(fn.resolve)
    assert fn.Matcher


def test_public_api_has_docstrings():
    import flakynet as fn

    names = [
        "Plan",
        "Latency",
        "Bandwidth",
        "Fragment",
        "Reset",
        "CloseAfter",
        "Blackhole",
        "RefuseConnect",
        "between",
        "nth",
        "every",
        "saw",
        "resolve",
        "Matcher",
    ]
    for name in names:
        doc = getattr(fn, name).__doc__
        assert doc, name
        assert doc.strip(), name


def test_latency_docstring_states_segment_caveat():
    import flakynet as fn

    doc = (fn.Latency.__doc__ or "").lower()
    assert "segment" in doc
    assert "chunk" in doc
