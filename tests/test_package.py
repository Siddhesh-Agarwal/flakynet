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
