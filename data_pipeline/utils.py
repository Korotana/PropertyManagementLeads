import re
from typing import Optional

def extract_beds(title: Optional[str]) -> Optional[int]:
    if not title:
        return None
    m = re.search(r"(\d+)\s*bed", title.lower())
    return int(m.group(1)) if m else None
