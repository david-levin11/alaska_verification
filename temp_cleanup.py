"""Best-effort cleanup of disposable model downloads."""
import shutil


def cleanup_model_downloads(directory):
    """Remove downloads without discarding extracted forecasts on a file lock.

    Continue cleaning other files if Windows or another process holds one open.
    Only filesystem errors are tolerated; decoding and archive-write errors
    remain fatal in their respective callers.
    """
    failures = []

    def onerror(function, path, exc_info):
        error = exc_info[1]
        if isinstance(error, FileNotFoundError):
            return
        if not isinstance(error, OSError):
            raise error
        failures.append(path)
        print(f"WARNING: Could not remove temporary path {path}: {error}")

    shutil.rmtree(directory, onerror=onerror)
    if failures:
        print(f"WARNING: Temporary downloads remain in {directory}. "
              "Continuing with extracted forecasts; cleanup can be retried after the file lock is released.")
    return not failures
