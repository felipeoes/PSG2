import json
from tqdm import tqdm
from pathlib import Path

CHECKPOINTS_DIR = Path("checkpoints_statistics")


def load_statistics(path: Path):
    """Loads the statistics from a file"""
    if not path.exists():
        return {}

    with open(path, "r") as f:
        statistics = json.load(f)
    return statistics


def load_statistics_from_checkpoints_dir(checkpoints_dir: Path):
    """Loads the statistics from a directory containing checkpoints"""
    full_statistics = {}
    filtered_statistics = {}
    for checkpoint_dir in tqdm(checkpoints_dir.iterdir()):
        # each dir has the filtered_statistics.json and full_statistics.json files
        full_statistics_path = checkpoint_dir / "full_statistics.json"
        filtered_statistics_path = checkpoint_dir / "filtered_statistics.json"

        # if full_statistics_path does nto exist, dir has subdirs with the statistics
        if not full_statistics_path.exists():
            for subdir in checkpoint_dir.iterdir():
                full_statistics_path = subdir / "full_statistics.json"
                filtered_statistics_path = subdir / "filtered_statistics.json"

                if full_statistics_path.exists():
                    full_statistics.update(load_statistics(full_statistics_path))

                    filtered_statistics.update(
                        load_statistics(filtered_statistics_path)
                    )

        full_statistics.update(load_statistics(full_statistics_path))

        filtered_statistics.update(load_statistics(filtered_statistics_path))

    return full_statistics, filtered_statistics


def get_sum_statistics(statistics: dict):
    """Sums the statistics from the statistics dict"""
    sum_statistics = {"n_pages": 0, "n_words": 0, "n_characters": 0}

    for key, value in statistics.items():
        _, n_pages, n_words, n_characters = value.values()
        sum_statistics["n_pages"] += n_pages
        sum_statistics["n_words"] += n_words
        sum_statistics["n_characters"] += n_characters

    return sum_statistics


if __name__ == "__main__":
    full_statistics, filtered_statistics = load_statistics_from_checkpoints_dir(
        CHECKPOINTS_DIR
    )

    print("Full statistics length:", len(full_statistics))
    print("Filtered statistics length:", len(filtered_statistics))

    sum_full_statistics = get_sum_statistics(full_statistics)

    print("Sum full statistics:", sum_full_statistics)

    sum_filtered_statistics = get_sum_statistics(filtered_statistics)

    print("Sum filtered statistics:", sum_filtered_statistics)
