import os
from pathlib import Path

def get_project_root() -> Path:
    if "PROJECT_ROOT" in os.environ:
        return Path(os.environ["PROJECT_ROOT"])
        
    if (Path.cwd() / "core").exists():
        return Path.cwd()
        
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "core").exists() or (parent / ".git").exists():
            return parent
            
    return current.parent