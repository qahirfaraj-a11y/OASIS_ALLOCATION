"""
Identity of the built-in SAMPLE data — one place, deliberately fictional.

The demo store network was modelled on a real customer's estate and carried
their chain name and their actual branch list. That data ships in every
release, in source form, so any client could read who the reference customer
was and which of their sites was the flagship. A demo must show the product,
not the customer.

The *shapes* are what matter and they are unchanged: a flagship carrying the
full range, an upscale curated store, a family bulk store, a mall express and
an urban impulse store. Only the identity is invented.

Swap these for a white-label deployment; ``oasis.logic.branding`` covers the
product name and colours, this covers the sample dataset.
"""

#: Fictional chain used by every sample store. Not a real retailer.
DEMO_CHAIN = "Meridian Fresh"

#: Locality names are invented too — a real suburb list next to a real chain
#: name is what made the old dataset identifiable.
DEMO_BRANCHES = (
    ("Parkview", "PARKVW", "Parkview Avenue, Central"),
    ("Highgrove", "HIGHGR", "Highgrove Road, North"),
    ("Oakridge", "OAKRDG", "Oakridge Lane, West"),
    ("Northgate Mall", "NGATE", "Northgate Shopping Mall"),
    ("Central Plaza", "CPLAZA", "Central Plaza, Downtown"),
)

#: The single-store sample is the flagship of the same fictional chain.
DEMO_SINGLE_BRANCH = DEMO_BRANCHES[0]

#: Market context. A city alone identifies nobody — thousands of retailers
#: trade here — but the branch names above must stay invented.
DEMO_CITY = "Nairobi"


def branch_name(index: int = 0) -> str:
    """``"Meridian Fresh - Parkview"`` for branch ``index``."""
    return f"{DEMO_CHAIN} - {DEMO_BRANCHES[index][0]}"


def single_store_name() -> str:
    return branch_name(0)


#: Tokens that must never appear in shipped sample data or the UI that names
#: it. Kept here so the guard test and the data have one shared definition.
IDENTIFYING_TOKENS = (
    "chandarana", "rhapta", "lavington", "westgate", "yaya centre",
    "adlife", "baba dogo", "kilimani",
)
