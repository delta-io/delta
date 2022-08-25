from pathlib import Path
# delta.io version
def get_version_from_sbt():
    version = Path("../../version.sbt").read_text().strip()
    return version.split('"')[1]

VERSION = get_version_from_sbt()

