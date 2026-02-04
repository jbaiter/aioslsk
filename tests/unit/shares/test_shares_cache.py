# from aioslsk.configuration import Configuration
from aioslsk.shares.cache import SharesShelveCache
from aioslsk.shares.model import SharedDirectory, SharedItem
import logging
import os
import shutil


RESOURCES = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'resources')

SHARED_DIRECTORY = SharedDirectory('music', 'C:\\music', 'abcdef')
SHARED_ITEMS = {
    'item1': SharedItem(SHARED_DIRECTORY, 'folk\\folkalbum, release\\', 'simple band(contrib. singer) - isn\'t easy song.mp3', 0.0, 0),
    'item2': SharedItem(SHARED_DIRECTORY, 'metal\\metalalbum release\\', 'simple band (contrib. singer)_-_don\'t easy song 片仮名.flac', 0.0, 0),
    'item3': SharedItem(SHARED_DIRECTORY, 'rap\\rapalbum_release\\', 'simple_band(contributer. singer)-don\'t_easy_song 片仮名.mp3', 0.0, 0),
}
SHARED_DIRECTORY.items = set(SHARED_ITEMS.values())


class TestSharesShelveCache:

    def test_read(self, tmpdir: str):
        shutil.copytree(os.path.join(RESOURCES, 'data'), tmpdir, dirs_exist_ok=True)
        cache = SharesShelveCache(tmpdir)
        directories = cache.read()
        assert [SHARED_DIRECTORY, ] == directories

    def test_write(self, tmpdir: str):
        cache = SharesShelveCache(tmpdir)
        cache.write([SHARED_DIRECTORY, ])

    def test_read_drops_legacy_unknown_filesize_entries(self, tmpdir: str, monkeypatch, caplog):
        cache = SharesShelveCache(tmpdir)

        directory = SharedDirectory('music', tmpdir, 'abcdef')
        item = SharedItem(directory, '', 'missing.mp3', 0.0, -1)
        directory.items = {item}
        cache.write([directory])

        monkeypatch.setattr(os.path, "getsize", lambda _p: (_ for _ in ()).throw(OSError("nope")))

        caplog.set_level(logging.WARNING, logger="aioslsk.shares.cache")
        directories = cache.read()
        assert len(directories) == 1
        assert directories[0].items == set()
        assert "Dropped 1 legacy share cache entries" in caplog.text
