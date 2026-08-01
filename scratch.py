import sqlite3
import pathlib
import os

base = pathlib.Path('/private/var/folders/n5/swn93jl93916r6vz3s4nykkw0000gn/T/pytest-of-jonathan/pytest-current')
test_dirs = [d for d in base.glob('test_second_run_reuses_complet*') if d.is_dir()]
latest_test_dir = max(test_dirs, key=os.path.getmtime)
db_path = latest_test_dir / "portable" / "data" / "notable.sqlite3"

db = sqlite3.connect(db_path)
db.row_factory = sqlite3.Row
for row in db.execute("SELECT * FROM attempt WHERE work_item_id = 7"):
    print(dict(row))
