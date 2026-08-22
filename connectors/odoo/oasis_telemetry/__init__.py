try:
    from . import models  # noqa: F401
except ImportError:
    # Running outside a live Odoo instance (unit tests, standalone XML-RPC
    # backfill): the pure ``mapping`` and ``push_client`` submodules must still
    # import. Inside Odoo, ``odoo`` is present and models load normally.
    pass
