"""Refresh the public score after verifying the frozen forecast is publicly readable."""

import argparse
import json
from pathlib import Path
from urllib.request import Request, urlopen

from prime_experiment import ORIGIN, save, score, verify_pending

PUBLIC_MANIFEST = "https://prime.notallow.cc/prime-prediction.json"


def progress(result):
    return [len(cell["windows"]) for cell in result["cells"]]


def refresh(pages, commit):
    path = pages / "prime-score.json"
    manifest_bytes = (pages / "prime-prediction.json").read_bytes()
    release = Path(__file__).resolve().parents[1] / "src/client_assets/prime-prediction.json"
    if release.read_bytes() != manifest_bytes:
        raise ValueError("Publish the matching model and prediction release before scoring")
    request = Request(PUBLIC_MANIFEST, headers={"User-Agent": "Prime-Research/1.0"})
    with urlopen(request, timeout=30) as response:
        if response.read() != manifest_bytes:
            raise ValueError("Public manifest differs from the Pages checkout")
    manifest = json.loads(manifest_bytes)
    if manifest["origin"] != ORIGIN:
        raise ValueError("The public score uses the production archive")
    previous = json.loads(path.read_bytes()) if path.exists() else {}
    if previous.get("manifest_sha256") != manifest["sha256"]:
        previous = {}
    publication = previous.get("publication")
    if not publication:
        # This happens after retrieving the public file. If coverage already began,
        # the experiment cannot retroactively receive a pre-publication witness.
        publication = dict(verify_pending(manifest), url=PUBLIC_MANIFEST, pages_commit=commit)
    elif publication.get("manifest_sha256") != manifest["sha256"]:
        raise ValueError("Published witness references a different prediction")
    elif all(not cell["pending"] for cell in previous["cells"]):
        return
    current = score(manifest, None)
    if previous.get("publication") and progress(previous) == progress(current):
        return
    current["publication"] = publication
    save(path, current, replace=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pages", type=Path)
    parser.add_argument("commit")
    args = parser.parse_args()
    refresh(args.pages, args.commit)
