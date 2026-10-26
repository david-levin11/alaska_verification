"""Filesystem cleanup must not prevent an otherwise successful archive write."""
import os

import pytest

from temp_cleanup import cleanup_model_downloads


def test_locked_file_warns_but_other_downloads_are_removed(tmp_path, monkeypatch, capsys):
    directory = tmp_path / 'downloads'
    directory.mkdir()
    locked = directory / 'locked.grib2'
    locked.write_bytes(b'locked')
    (directory / 'other.grib2').write_bytes(b'removable')
    original_unlink = os.unlink

    def unlink(path, *args, **kwargs):
        if str(path).endswith('locked.grib2'):
            raise PermissionError(13, 'File is being used by another process', str(path))
        return original_unlink(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(os, 'unlink', unlink)
        assert cleanup_model_downloads(directory) is False
    assert locked.exists()
    assert not (directory / 'other.grib2').exists()
    assert str(locked) in capsys.readouterr().out
    assert cleanup_model_downloads(directory) is True
    assert not directory.exists()
    assert cleanup_model_downloads(directory) is True


def test_non_filesystem_failures_are_not_suppressed(tmp_path, monkeypatch):
    directory = tmp_path / 'downloads'
    directory.mkdir()
    (directory / 'file').touch()

    def broken(*args, **kwargs):
        raise RuntimeError('unexpected programming error')

    monkeypatch.setattr(os, 'unlink', broken)
    with pytest.raises(RuntimeError, match='unexpected programming error'):
        cleanup_model_downloads(directory)
