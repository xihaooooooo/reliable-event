"""Archive one matrix and all of its raw runs, including process logs."""

import hashlib
import sys
import zipfile
from pathlib import Path


def main(results_dir: Path, prefix: str, destination: Path) -> None:
    runs = sorted(path for path in results_dir.glob(prefix + "-*") if path.is_dir())
    if not runs:
        raise SystemExit(f"No run directories found for {prefix}")
    matrix = [results_dir / f"{prefix}-matrix.{suffix}" for suffix in ("csv", "md")]
    if any(not path.is_file() for path in matrix):
        raise SystemExit("Matrix reports are missing; finish aggregation before packaging")
    files = matrix + [file for run in runs for file in sorted(run.rglob("*")) if file.is_file()]
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED,
                         compresslevel=6, allowZip64=True) as archive:
        for file in files:
            archive.write(file, Path("results") / file.relative_to(results_dir))
    checksum = hashlib.sha256()
    with destination.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(block)
    digest = checksum.hexdigest()
    destination.with_suffix(destination.suffix + ".sha256").write_text(
        f"{digest}  {destination.name}\n", encoding="utf-8")
    print(f"{len(runs)} runs, {len(files)} files: {destination} SHA-256 {digest}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("Usage: package-results.py RESULTS_DIR PREFIX OUTPUT_ZIP")
    main(Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3]))
