import json
import hashlib
import os
import logging
from typing import Dict, Any

logger = logging.getLogger("OASIS.LicenseManager")

class OfflineLicenseManager:
    """
    Manages offline, on-premise license verification.
    This module prevents unauthorized runtime of standalone modules without an active license.
    """
    def __init__(self, key_path: str = "/data/oasis_license.key"):
        self.key_path = key_path
        # Signing salt must be supplied via environment — never shipped in source.
        self._salt = os.getenv("OASIS_LICENSE_SALT", "")
        if not self._salt:
            logger.error(
                "OASIS_LICENSE_SALT is not set. License signatures cannot be "
                "verified; licensed modules will fail verification until it is configured."
            )

    def _generate_fingerprint(self, tenant_id: str, module_name: str, expiry_date: str) -> str:
        raw = f"{tenant_id}:{module_name}:{expiry_date}:{self._salt}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def verify_license(self, module_name: str) -> bool:
        """
        Reads the offline license key and verifies if the specified module is authorized to run.
        """
        if not os.path.exists(self.key_path):
            logger.warning(f"No license key found at {self.key_path}. Defaulting to evaluation mode or locking down.")
            # For this Phase, we'll allow standard modules to pass if no key exists (evaluation mode),
            # but premium modules like 'allocation' or ERP adapters would fail.
            return module_name in ["ops", "stgat", "shadow", "approval"]

        try:
            with open(self.key_path, 'r') as f:
                license_data = json.load(f)
            
            tenant = license_data.get("tenant_id")
            expiry = license_data.get("expiry_date")
            modules = license_data.get("authorized_modules", {})

            if module_name not in modules:
                logger.error(f"Module '{module_name}' is not licensed for this tenant.")
                return False

            expected_signature = self._generate_fingerprint(tenant, module_name, expiry)
            provided_signature = modules.get(module_name)

            if expected_signature != provided_signature:
                logger.error("License signature mismatch! Key may be tampered with.")
                return False
            
            # Note: A complete implementation would also verify expiry_date against current time.
            logger.info(f"License for module '{module_name}' verified successfully.")
            return True

        except Exception as e:
            logger.error(f"Failed to read license key: {e}")
            return False

def verify_module_startup(module_name: str) -> bool:
    """Helper function to run on application startup."""
    manager = OfflineLicenseManager()
    return manager.verify_license(module_name)
