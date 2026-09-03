from pathlib import Path

import adsb


def test_adsb_package_resolves_to_src_adsb():
    """The new pipeline is importable as ``adsb`` from the ``src`` root.

    Guards the layout: the legacy app imports its modules flat
    (``from logic import ...``), so ``src`` is the import root and the new
    pipeline must live in its own package under it rather than adding more
    top-level modules.
    """
    package_dir = Path(adsb.__file__).parent
    assert package_dir.name == "adsb"
    assert package_dir.parent.name == "src"
