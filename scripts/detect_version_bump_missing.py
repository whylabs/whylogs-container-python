import os
import sys
from typing import List

from packaging.version import Version, parse


def get_latest_release_note_version(release_notes_dir: str) -> Version:
    # Get a list of all markdown files in the release notes directory
    files = os.listdir(release_notes_dir)
    # Extract version numbers from filenames, parse them, and store them in a list
    versions: List[Version] = [parse(f.split(".md")[0]) for f in files]
    # Return the highest version found
    return max(versions)


def main(current_version: str, release_notes_dir: str) -> None:
    # Parse the current version, considering it as a complete string including development tags if present
    current_version_parsed: Version = parse(current_version)
    # Get the latest published version from the release notes directory
    latest_version: Version = get_latest_release_note_version(release_notes_dir)

    # Compare the current version with the latest version
    if current_version_parsed < latest_version:
        print(
            f"Release notes for found for {latest_version} but the current package version is {current_version_parsed}."
            " Bump the package version to match the latest release notes."
        )
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <current_version>")
        sys.exit(1)

    current_version = sys.argv[1]
    release_notes_dir = "./release_notes"

    main(current_version, release_notes_dir)
