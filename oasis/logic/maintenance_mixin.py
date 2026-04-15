import logging
import os
import glob
from datetime import datetime, timedelta

logger = logging.getLogger("OrderEngine.Maintenance")

class MaintenanceMixin:
    """
    MaintenanceMixin handles log rotation, temporary file cleanup, 
    and database indexing maintenance.
    """
    def rotate_logs(self, log_dir: str, keep_days: int = 30):
        """Cleanup old logs beyond retention period."""
        logger.info(f"Maintenance: Rotating logs in {log_dir} (Retention: {keep_days} days)")
        threshold = datetime.now() - timedelta(days=keep_days)
        
        try:
            for log_file in glob.glob(os.path.join(log_dir, "*.log")):
                mtime = datetime.fromtimestamp(os.path.getmtime(log_file))
                if mtime < threshold:
                    os.remove(log_file)
                    logger.info(f"Deleted old log: {os.path.basename(log_file)}")
        except Exception as e:
            logger.error(f"Maintenance failed: {e}")

    def cleanup_temp_exports(self, export_dir: str):
        """Cleanup temporary Excel/CSV exports."""
        # Standard maintenance logic from Golden State v3
        pass
