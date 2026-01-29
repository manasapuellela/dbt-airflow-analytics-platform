import site
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

site.addsitedir(str(PROJECT_ROOT))
