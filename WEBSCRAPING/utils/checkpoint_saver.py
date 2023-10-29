import os
import json
from pathlib import Path
from unidecode import unidecode

class CheckpointSaver:
    """Class for saving and loading checkpoints for scrapers. JSON format is used."""

    def __init__(self, checkpoint_path):
        self.checkpoint_path = checkpoint_path

    def save_checkpoint(self, checkpoint: dict, filt: str):
        """Save checkpoint to file."""

        # check if dirs and subdirs in checkpoint_path exists or create them
        path = Path(self.checkpoint_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        # read checkpoint
        if os.path.exists(self.checkpoint_path):
            with open(self.checkpoint_path, "r") as f:
                checkpoint_json = json.load(f)

        else:
            checkpoint_json = {}

        # Save checkpoint
        checkpoint_json[unidecode(filt)] = checkpoint

        with open(self.checkpoint_path, "w") as f:
            json.dump(checkpoint_json, f, indent=4)

    def load_checkpoint(self, filt: str):
        """Load checkpoint from file."""
        if not os.path.exists(self.checkpoint_path):
            return None

        with open(self.checkpoint_path, "r") as f:
            checkpoint = json.load(f)

        filt = unidecode(filt)
        if filt in checkpoint:
            return checkpoint[filt]

        return None
