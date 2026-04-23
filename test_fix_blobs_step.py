from lib.steps.s04_fix_blobs import FixBlobsStep
from lib.db import MigrationDB
from lib.config import MigrationConfig
from pathlib import Path

mig_seq = "0001"
mig_dir = Path(f"MIGRACAO_{mig_seq}")
db = MigrationDB(mig_dir / "migration.db")
config = MigrationConfig(mig_dir / "config.yaml")

# Get migration id
mig_info = db.get_migration_by_seq(mig_seq)
step = FixBlobsStep(mig_info['id'], db, config, 4)

print(f"Running FixBlobsStep for migration {mig_seq}...")
success = step.run()
print(f"Success: {success}")
