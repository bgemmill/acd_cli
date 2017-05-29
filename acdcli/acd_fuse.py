"""fusepy filesystem module"""

import configparser
import errno
import io
import json
import logging
import os
import stat
import sys
import tempfile

from collections import deque, defaultdict
from multiprocessing import Process
from threading import Lock, Thread
from time import time, sleep

import ctypes.util
import binascii

import requests

from acdcli.cache.db import CacheConsts

from fuse import FUSE, FuseOSError as FuseError, Operations
from acdcli.api.common import RequestError
from acdcli.utils.conf import get_conf

ctypes.util.__find_library = ctypes.util.find_library

def find_library(*args):
    if 'fuse' in args[0]:
        libfuse_path = os.environ.get('LIBFUSE_PATH')
        if libfuse_path:
            return libfuse_path

    return ctypes.util.__find_library(*args)

ctypes.util.find_library = find_library
logger = logging.getLogger(__name__)

try:
    errno.ECOMM
except:
    errno.ECOMM = errno.ECONNABORTED
try:
    errno.EREMOTEIO
except:
    errno.EREMOTEIO = errno.EIO

_SETTINGS_FILENAME = 'fuse.ini'
_XATTR_PROPERTY_NAME = 'xattrs'
_XATTR_MTIME_OVERRIDE_NAME = 'fuse.mtime'
_XATTR_MODE_OVERRIDE_NAME = 'fuse.mode'
_XATTR_UID_OVERRIDE_NAME = 'fuse.uid'
_XATTR_GID_OVERRIDE_NAME = 'fuse.gid'
_XATTR_SYMLINK_OVERRIDE_NAME = 'fuse.symlink'
_XATTR_DELAY = 2  # seconds to wait for additional xattr changes before flushing to amazon

_def_conf = configparser.ConfigParser()
_def_conf['fs'] = dict(block_size=io.DEFAULT_BUFFER_SIZE)
_def_conf['read'] = dict(open_chunk_limit=10, timeout=5, cache_small_file_size=1024)
_def_conf['write'] = dict(buffer_size=int(1e9), timeout=30)


class FuseOSError(FuseError):
    def __init__(self, err_no: int):
        # logger.debug('FUSE error %i, %s.' % (err_no, errno.errorcode[err_no]))
        super().__init__(err_no)

    CODE = RequestError.CODE
    codes = RequestError.codes
    code_mapping = {CODE.CONN_EXCEPTION: FuseError(errno.ECOMM),
                    codes.CONFLICT: FuseError(errno.EEXIST),
                    codes.REQUESTED_RANGE_NOT_SATISFIABLE: FuseError(errno.EFAULT),
                    codes.REQUEST_TIMEOUT: FuseError(errno.ETIMEDOUT),
                    codes.GATEWAY_TIMEOUT: FuseError(errno.ETIMEDOUT)
                    }

    @staticmethod
    def convert(e: RequestError):
        """:raises: FuseOSError"""

        try:
            caller = sys._getframe().f_back.f_code.co_name + ': '
        except AttributeError:
            caller = ''
        logger.error(caller + e.__str__())

        try:
            exc = FuseOSError.code_mapping[e.status_code]
        except AttributeError:
            exc = FuseOSError(errno.EREMOTEIO)
        raise exc


class ReadProxy(object):
    """Dict of stream chunks for consecutive read access of files."""

    def __init__(self, acd_client, open_chunk_limit, timeout):
        self.acd_client = acd_client
        self.lock = Lock()
        self.files = defaultdict(lambda: ReadProxy.ReadFile(open_chunk_limit, timeout))

    class StreamChunk(object):
        """StreamChunk represents a file node chunk as a streamed ranged HTTP response
        which may or may not be partially read."""

        __slots__ = ('offset', 'r', 'end')

        def __init__(self, acd_client, id_, offset, length, **kwargs):
            self.offset = offset
            """the first byte position (fpos) available in the chunk"""

            self.r = acd_client.response_chunk(id_, offset, length, **kwargs)
            """:type: requests.Response"""

            self.end = offset + int(self.r.headers['content-length']) - 1
            """the last byte position (fpos) contained in the chunk"""

        def has_byte_range(self, offset, length) -> bool:
            """Tests whether chunk begins at **offset** and has at least **length** bytes remaining.
            """
            logger.debug('s: %d-%d; r: %d-%d'
                         % (self.offset, self.end, offset, offset + length - 1))
            if offset == self.offset and offset + length - 1 <= self.end:
                return True
            return False

        def get(self, length) -> bytes:
            """Gets *length* bytes beginning at current offset.

            :param length: the number of bytes to get
            :raises: Exception if less than *length* bytes were received \
             but end of chunk was not reached"""

            b = next(self.r.iter_content(length))
            self.offset += len(b)

            if len(b) < length and self.offset <= self.end:
                logger.warning('Chunk ended unexpectedly.')
                raise Exception
            return b

        def close(self):
            """Closes connection on the stream."""
            self.r.close()

    class ReadFile(object):
        """Represents a file opened for reading.
        Encapsulates at most :attr:`MAX_CHUNKS_PER_FILE` open chunks."""

        __slots__ = ('chunks', 'access', 'lock', 'timeout')

        def __init__(self, open_chunk_limit, timeout):
            self.chunks = deque(maxlen=open_chunk_limit)
            self.access = time()
            self.lock = Lock()
            self.timeout = timeout

        def get(self, acd_client, id_, offset, length, total) -> bytes:
            """Gets a byte range from existing StreamChunks"""

            with self.lock:
                i = len(self.chunks) - 1
                while i >= 0:
                    c = self.chunks[i]
                    if c.has_byte_range(offset, length):
                        try:
                            bytes_ = c.get(length)
                        except:
                            self.chunks.remove(c)
                        else:
                            return bytes_
                    i -= 1

            try:
                with self.lock:
                    chunk = ReadProxy.StreamChunk(acd_client, id_, offset,
                                                  acd_client._conf.getint('transfer',
                                                                          'dl_chunk_size'),
                                                  timeout=self.timeout)
                    if len(self.chunks) == self.chunks.maxlen:
                        self.chunks[0].close()

                    self.chunks.append(chunk)
                    return chunk.get(length)
            except RequestError as e:
                FuseOSError.convert(e)

        def clear(self):
            """Closes chunks and clears chunk deque."""
            with self.lock:
                for chunk in self.chunks:
                    try:
                        chunk.close()
                    except:
                        pass
                self.chunks.clear()

    def get(self, id_, offset, length, total):
        with self.lock:
            f = self.files[id_]
        return f.get(self.acd_client, id_, offset, length, total)

    def invalidate(self):
        pass

    def release(self, id_):
        with self.lock:
            f = self.files.get(id_)
        if f:
            f.clear()


class WriteProxy(object):
    """Collection of WriteStreams for consecutive file write operations."""

    def __init__(self, acd_client, cache, buffer_size, timeout):
        self.acd_client = acd_client
        self.cache = cache
        self.buffers = defaultdict(lambda: WriteProxy.WriteBuffer(buffer_size))

    class WriteBuffer(object):
        def __init__(self, buffer_size):
            self.f = tempfile.SpooledTemporaryFile(max_size=buffer_size)
            self.lock = Lock()
            self.dirty = True
            self.len = 0

        def read(self, offset, length: int):
            with self.lock:
                self.f.seek(offset)
                return self.f.read(length)

        def write(self, offset, bytes_: bytes):
            with self.lock:
                self.dirty = True
                self.f.seek(offset)
                ret = self.f.write(bytes_)
                self.f.seek(0, os.SEEK_END)
                self.len = self.f.tell()
                return ret

        def length(self):
            return self.len

        def get_file(self):
            """Return the file for direct access. Be sure to lock from the outside when doing so"""
            self.f.seek(0)
            return self.f

    def _write_and_sync(self, buffer: WriteBuffer, node_id: str):
        try:
            with buffer.lock:
                if not buffer.dirty:
                    return
                r = self.acd_client.overwrite_tempfile(node_id, buffer.get_file())
                buffer.dirty = False
        except (RequestError, IOError) as e:
            logger.error('Error writing node "%s". %s' % (node_id, str(e)))
        else:
            self.cache.insert_node(r, flush_resolve_cache=False)

    def read(self, node_id, fh, offset, length: int):
        b = self.buffers.get(node_id)
        if b:
            return b.read(offset, length)

    def write(self, node_id, fh, offset, bytes_: bytes):
        """Gets WriteBuffer from defaultdict.

        :raises: FuseOSError: wrong offset or writing failed"""

        b = self.buffers[node_id]
        b.write(offset, bytes_)

    def length(self, node_id, fh):
        b = self.buffers.get(node_id)
        if b:
            return b.length()

    def flush(self, node_id, fh):
        b = self.buffers.get(node_id)
        if b:
            self._write_and_sync(b, node_id)

    def release(self, node_id, fh):
        b = self.buffers.get(node_id)
        if b:
            self._write_and_sync(b, node_id)
            del self.buffers[node_id]

    def remove(self, node_id, fh):
        b = self.buffers.get(node_id)
        if b:
            del self.buffers[node_id]


class LoggingMixIn(object):
    """Modified fusepy LoggingMixIn that does not log read or written bytes
    and nicely formats non-decimal based arguments."""

    def __call__(self, op, path, *args):
        targs = None
        if op == 'open':
            targs = (('0x%0*x' % (4, args[0]),) + args[1:])
        elif op == 'write':
            targs = (len(args[0]),) + args[1:]
        elif op == 'chmod':
            targs = (oct(args[0]),) + args[1:]
        elif op == 'setxattr':
            targs = (args[0],) + (len(args[1]),)

        logger.debug('-> %s %s %s', op, path, repr(args if not targs else targs))

        ret = '[Unhandled Exception]'
        try:
            ret = getattr(self, op)(path, *args)
            return ret
        except OSError as e:
            ret = str(e)
            raise
        finally:
            if op == 'read':
                ret = len(ret)
            elif op == 'getxattr' and ret and ret != '[Errno 61] No data available':
                ret = len(ret)
            logger.debug('<- %s %s', op, repr(ret))


class ACDFuse(LoggingMixIn, Operations):
    """FUSE filesystem operations class for Amazon Cloud Drive.
    See `<http://fuse.sourceforge.net/doxygen/structfuse__operations.html>`_."""

    def __init__(self, **kwargs):
        """Calculates ACD usage and starts autosync process.

        :param kwargs: cache (NodeCache), acd_client (ACDClient), autosync (partial)"""

        self.xattr_cache = {}
        self.xattr_dirty = set()
        self.xattr_cache_lock = Lock()

        self.cache = kwargs['cache']
        self.acd_client = kwargs['acd_client']
        self.acd_client_owner = self.cache.KeyValueStorage.get(CacheConsts.OWNER_ID)
        autosync = kwargs['autosync']
        conf = kwargs['conf']
        self.conf = conf

        self.rp = ReadProxy(self.acd_client,
                            conf.getint('read', 'open_chunk_limit'), conf.getint('read', 'timeout'))
        """collection of files opened for reading"""
        self.wp = WriteProxy(self.acd_client, self.cache,
                             conf.getint('write', 'buffer_size'), conf.getint('write', 'timeout'))
        """collection of files opened for writing"""
        try:
            total, _ = self.acd_client.fs_sizes()
        except RequestError:
            logger.warning('Error getting account quota data. '
                           'Cannot determine total and available disk space.')
            total = 0

        self.total = total
        """total disk space"""
        self.free = 0 if not total else total - self.cache.calculate_usage()
        """manually calculated available disk space"""
        self.fh = 1
        """file handle counter\n\n :type: int"""
        self.fh_to_node = {}
        """map fh->node_id\n\n :type: dict"""
        self.node_to_fh = defaultdict(lambda: set())
        """map node_id to list of interested file handles"""
        self.fh_lock = Lock()
        """lock for fh counter increment and handle dict writes"""
        self.nlinks = kwargs.get('nlinks', False)
        """whether to calculate the number of hardlinks for folders"""
        self.uid = kwargs['uid']
        """sets the default uid"""
        self.gid = kwargs['gid']
        """sets the default gid"""
        self.umask = kwargs['umask']
        """sets the default umask"""
        self.cache_small_file_size = conf.getint('read', 'cache_small_file_size')
        """size of files under which we cache the contents automatically"""

        self.destroyed = autosync.keywords['stop']
        """:type: multiprocessing.Event"""

        p = Process(target=autosync)
        p.start()

    def destroy(self, path):
        self.destroyed.set()

    def readdir(self, path, fh) -> 'List[str]':
        """Lists the path's contents.

        :raises: FuseOSError if path is not a node or path is not a folder"""

        node = self.cache.resolve(path)
        if not node:
            raise FuseOSError(errno.ENOENT)
        if not node.type == 'folder':
            raise FuseOSError(errno.ENOTDIR)

        folders, files = self.cache.list_children(folder_id=node.id, folder_path=path)
        return [_ for _ in ['.', '..'] + [c.name for c in folders + files]]

    def getattr(self, path, fh=None) -> dict:
        """Creates a stat-like attribute dict, see :manpage:`stat(2)`.
        Calculates correct number of links for folders if :attr:`nlinks` is set."""

        if fh:
            node_id = self.fh_to_node[fh]
            node = self.cache.get_node(node_id)
        else:
            node = self.cache.resolve(path)
        if not node:
            raise FuseOSError(errno.ENOENT)
        return self._getattr(node, fh)

    def _getattr(self, node, fh=None) -> dict:
        try: mtime = self._getxattr(node.id, _XATTR_MTIME_OVERRIDE_NAME)
        except: mtime = node.modified.timestamp()

        size = self.wp.length(node.id, fh)
        if size is None: size = node.size

        try: uid = self._getxattr(node.id, _XATTR_UID_OVERRIDE_NAME)
        except: uid = self.uid

        try: gid = self._getxattr(node.id, _XATTR_GID_OVERRIDE_NAME)
        except: gid = self.gid

        attrs = dict(st_atime=time(),
                     st_mtime=mtime,
                     st_ctime=node.created.timestamp(),
                     st_uid=uid,
                     st_gid=gid)

        try: mode = self._getxattr(node.id, _XATTR_MODE_OVERRIDE_NAME)
        except: mode = None

        if node.is_folder:
            # directory
            mode = stat.S_IFDIR | (stat.S_IMODE(mode) if mode else 0o0777 & ~self.umask)

            return dict(st_mode=mode,
                        st_nlink=self.cache.num_children(node.id) if self.nlinks else 1,
                        **attrs)
        elif node.is_file:
            # symlink
            if mode and stat.S_ISLNK(mode): mode = stat.S_IFLNK | 0o0777
            # file
            else: mode = stat.S_IFREG | (stat.S_IMODE(mode) if mode else 0o0666 & ~self.umask)

            return dict(st_mode=mode,
                        st_nlink=self.cache.num_parents(node.id) if self.nlinks else 1,
                        st_size=size,
                        st_blksize=self.conf.getint('fs', 'block_size'),
                        st_blocks=(size + 511) // 512,  # this field always expects a 512 block size
                        **attrs)

    def listxattr(self, path):
        node_id = self.cache.resolve_id(path)
        if not node_id:
            raise FuseOSError(errno.ENOENT)
        return self._listxattr(node_id)

    def _listxattr(self, node_id):
        self._xattr_load(node_id)
        with self.xattr_cache_lock:
            try:
                return [k for k, v in self.xattr_cache[node_id].items()]
            except:
                return []

    def getxattr(self, path, name, position=0):
        node_id = self.cache.resolve_id(path)
        if not node_id:
            raise FuseOSError(errno.ENOENT)
        return self._getxattr_bytes(node_id, name)

    def _getxattr(self, node_id, name):
        self._xattr_load(node_id)
        with self.xattr_cache_lock:
            try:
                ret = self.xattr_cache[node_id][name]
                if ret is not None:
                    return ret
            except:
                pass
            raise FuseOSError(errno.ENODATA)  # should be ENOATTR

    def _getxattr_bytes(self, node_id, name):
        return binascii.a2b_base64(self._getxattr(node_id, name))

    def removexattr(self, path, name):
        node_id = self.cache.resolve_id(path)
        if not node_id:
            raise FuseOSError(errno.ENOENT)
        self._removexattr(node_id, name)

    def _removexattr(self, node_id, name):
        self._xattr_load(node_id)
        with self.xattr_cache_lock:
            if name in self.xattr_cache[node_id]:
                del self.xattr_cache[node_id][name]
                self.xattr_dirty.add(node_id)

    def setxattr(self, path, name, value, options, position=0):
        node_id = self.cache.resolve_id(path)
        if not node_id:
            raise FuseOSError(errno.ENOENT)
        self._setxattr_bytes(node_id, name, value)

    def _setxattr(self, node_id, name, value):
        self._xattr_load(node_id)
        with self.xattr_cache_lock:
            try:
                self.xattr_cache[node_id][name] = value
                self.xattr_dirty.add(node_id)
            except:
                raise FuseOSError(errno.ENOTSUP)

    def _setxattr_bytes(self, node_id, name, value: bytes):
        self._setxattr(node_id, name, binascii.b2a_base64(value).decode("utf-8"))

    def _xattr_load(self, node_id):
        with self.xattr_cache_lock:
            if node_id not in self.xattr_cache:
                xattrs_str = self.cache.get_property(node_id, self.acd_client_owner, _XATTR_PROPERTY_NAME)
                try: self.xattr_cache[node_id] = json.loads(xattrs_str)
                except: self.xattr_cache[node_id] = {}

    def _xattr_flush(self, node_id):
        # collect all xattr changes while any fh's are open so we talk to amazon less
        with self.fh_lock:
            if self.node_to_fh.get(node_id):
                return
        Thread(target=self._xattr_write_and_sync, args=(node_id,)).start()

    def _xattr_write_and_sync(self, node_id):
        # try to collect many xattr changes at once so we talk to amazon less
        sleep(_XATTR_DELAY)
        with self.xattr_cache_lock:
            if node_id in self.xattr_dirty:
                try:
                    xattrs_str = json.dumps(self.xattr_cache[node_id])
                    self.acd_client.add_property(node_id, self.acd_client_owner, _XATTR_PROPERTY_NAME,
                                                 xattrs_str)
                except (RequestError, IOError) as e:
                    logger.error('Error writing node xattrs "%s". %s' % (node_id, str(e)))
                    try: del self.xattr_cache[node_id]
                    except: pass
                else:
                    self.cache.insert_property(node_id, self.acd_client_owner, _XATTR_PROPERTY_NAME, xattrs_str)
                self.xattr_dirty.discard(node_id)

    def read(self, path, length, offset, fh=None) -> bytes:
        """Read ```length`` bytes from ``path`` at ``offset``."""

        if fh:
            node_id = self.fh_to_node[fh]
            node = self.cache.get_node(node_id)
        else:
            node = self.cache.resolve(path)
        if not node:
            raise FuseOSError(errno.ENOENT)

        size = self.wp.length(node.id, fh)
        if size is None: size = node.size

        if size <= offset:
            return b''

        if size < offset + length:
            length = size - offset

        """If we attempt to read something we just wrote, give it back"""
        ret = self.wp.read(node.id, fh, offset, length)
        if ret is not None:
            return ret

        """Next, check our local cache"""
        content = self.cache.get_content(node.id, node.version)
        if content is not None:
            return content[offset:offset+length]

        """For small files, read and cache the whole file"""
        if node.size <= self.cache_small_file_size:
            content = self.acd_client.download_chunk(node.id, 0, node.size)
            self.cache.insert_content(node.id, node.version, content)
            return content[offset:offset+length]

        """For all other files, stream from amazon"""
        return self.rp.get(node.id, offset, length, node.size)

    def statfs(self, path) -> dict:
        """Gets some filesystem statistics as specified in :manpage:`statfs(2)`."""

        bs = self.conf.getint('fs', 'block_size')
        return dict(f_bsize=bs,
                    f_frsize=bs,
                    f_blocks=self.total // bs,  # total no of blocks
                    f_bfree=self.free // bs,  # free blocks
                    f_bavail=self.free // bs,
                    f_namemax=256
                    )

    def mkdir(self, path, mode):
        """Creates a directory at ``path`` (see :manpage:`mkdir(2)`)."""

        name = os.path.basename(path)
        ppath = os.path.dirname(path)
        p_id = self.cache.resolve_id(ppath)
        if not p_id:
            raise FuseOSError(errno.ENOTDIR)

        try:
            r = self.acd_client.create_folder(name, p_id)
        except RequestError as e:
            FuseOSError.convert(e)
        else:
            self.cache.insert_node(r, flush_resolve_cache=False)
            node_id = r['id']
            self.cache.resolve_cache_add(path, node_id)
            if mode is not None:
                self._setxattr(node_id, _XATTR_MODE_OVERRIDE_NAME, stat.S_IFDIR | (stat.S_IMODE(mode)))
                self._xattr_flush(node_id)

    def _trash(self, path):
        logger.debug('trash %s' % path)
        node = self.cache.resolve(path, False)

        if not node:  # or not parent:
            raise FuseOSError(errno.ENOENT)

        try:
            # if len(node.parents) > 1:
            #     r = metadata.remove_child(parent.id, node.id)
            # else:
            r = self.acd_client.move_to_trash(node.id)
        except RequestError as e:
            FuseOSError.convert(e)
        else:
            self.cache.insert_node(r, flush_resolve_cache=False)
            self.cache.resolve_cache_del(path)

    def rmdir(self, path):
        """Moves a directory into ACD trash."""
        self._trash(path)

    def unlink(self, path):
        """Moves a file into ACD trash."""
        self._trash(path)

    def create(self, path, mode, **kwargs) -> int:
        """Creates an empty file at ``path`` with access ``mode``.

        :param mode:
        :param path:
        :returns int: file handle"""

        name = os.path.basename(path)
        ppath = os.path.dirname(path)
        p_id = self.cache.resolve_id(ppath, False)
        if not p_id:
            raise FuseOSError(errno.ENOTDIR)

        try:
            r = self.acd_client.create_file(name, p_id)
            self.cache.insert_node(r, flush_resolve_cache=False)
            node_id = r['id']
            self.cache.resolve_cache_add(path, node_id)
        except RequestError as e:
            # file all ready exists, see what we know about it since the
            # cache may be out of sync or amazon missed a rename
            if e.status_code == requests.codes.conflict:
                prior_node_id = json.loads(e.msg)["info"]["nodeId"]
                logger.error('create: duplicate name: %s prior_node_id: %s' % (name, prior_node_id))
                prior_node_amazon = self.acd_client.get_metadata(prior_node_id, False, False)
                logger.error('create: prior_node(amazon): %s' % str(prior_node_amazon))
                prior_node_cache = self.cache.get_node(prior_node_id)
                logger.error('create: prior_node(cache): %s' % str(prior_node_cache))
                # if prior_node_cache.name != prior_node_amazon["name"]:
                #     self._rename(prior_node_id, prior_node_cache.name)
            FuseOSError.convert(e)

        if mode is not None:
            self._setxattr(node_id, _XATTR_MODE_OVERRIDE_NAME, stat.S_IFREG | (stat.S_IMODE(mode)))

        with self.fh_lock:
            self.fh += 1
            self.fh_to_node[self.fh] = node_id
            self.node_to_fh[node_id].add(self.fh)
        return self.fh

    def rename(self, old, new):
        """Renames ``old`` into ``new`` (may also involve a move).
        If ``new`` is an existing file, it is moved into the ACD trash.

        :raises FuseOSError: ENOENT if ``old`` is not a node, \
        EEXIST if ``new`` is an existing folder \
        ENOTDIR if ``new``'s parent path does not exist"""

        if old == new:
            return

        node = self.cache.resolve(old, False)
        if not node:
            raise FuseOSError(errno.ENOENT)

        new_bn, new_dn = os.path.basename(new), os.path.dirname(new)
        old_bn, old_dn = os.path.basename(old), os.path.dirname(old)

        existing = self.cache.resolve(new, False)
        if existing:
            if existing.is_file:
                self._trash(new)
            else:
                raise FuseOSError(errno.EEXIST)

        self.cache.resolve_cache_del(old)

        if new_bn != old_bn:
            self._rename(node, new_bn)

        if new_dn != old_dn:
            # odir_id = self.cache.resolve_path(old_dn, False)
            ndir = self.cache.resolve(new_dn, False)
            if not ndir:
                raise FuseOSError(errno.ENOTDIR)
            self._move(node, ndir.id)

        self.cache.resolve_cache_add(new, node.id)

    def _rename(self, node, name):
        try:
            r = self.acd_client.rename_node(node.id, name)
        except RequestError as e:
            FuseOSError.convert(e)
        else:
            self.cache.insert_node(r, flush_resolve_cache=node.is_folder)

    def _move(self, node, new_folder):
        try:
            r = self.acd_client.move_node(node.id, new_folder)
        except RequestError as e:
            FuseOSError.convert(e)
        else:
            self.cache.insert_node(r, flush_resolve_cache=node.is_folder)

    def open(self, path, flags) -> int:
        """Opens a file.

        :param path:
        :param flags: flags defined as in :manpage:`open(2)`
        :returns: file handle"""

        if (flags & os.O_APPEND) == os.O_APPEND:
            raise FuseOSError(errno.EFAULT)

        node_id = self.cache.resolve_id(path, False)
        if not node_id:
            raise FuseOSError(errno.ENOENT)
        return self._open(node_id)

    def _open(self, node_id):
        with self.fh_lock:
            self.fh += 1
            self.fh_to_node[self.fh] = node_id
            self.node_to_fh[node_id].add(self.fh)
        return self.fh

    def write(self, path, data, offset, fh) -> int:
        """Invokes :attr:`wp`'s write function.

        :returns: number of bytes written"""

        if fh:
            node_id = self.fh_to_node[fh]
        else:
            # This is not resolving by path on purpose, since flushing to
            # amazon is done on closing all interested file handles.
            node_id = None
        if not node_id:
            raise FuseOSError(errno.ENOENT)

        self.wp.write(node_id, fh, offset, data)
        return len(data)

    def flush(self, path, fh):
        if fh:
            node_id = self.fh_to_node[fh]
        else:
            node_id = self.cache.resolve_id(path)
        if not node_id:
            raise FuseOSError(errno.ENOENT)
        self.wp.flush(node_id, fh)

    def truncate(self, path, length, fh=None):
        """Pseudo-truncates a file, i.e. clears content if ``length``==0 or grows
        newly created nodes if ``length`` is greater than the write-back cache size.

        :raises FuseOSError: if pseudo-truncation to length is not supported"""

        if fh:
            node_id = self.fh_to_node[fh]
            node = self.cache.get_node(node_id)
        else:
            node = self.cache.resolve(path)
        if not node:
            raise FuseOSError(errno.ENOENT)

        # cut file size to 0
        if length == 0:
            if node.size:
                try:
                    r = self.acd_client.clear_file(node.id)
                except RequestError as e:
                    raise FuseOSError.convert(e)
                else:
                    self.cache.insert_node(r, flush_resolve_cache=False)
            self.wp.remove(node.id, None)
            return 0

        # grow newly created files
        if node.size == 0 and length:
            size = self.wp.length(node.id, fh)
            if size is None: size = node.size
            if length > size:
                # amazon doesn't understand sparse files, so we send zeros
                internal_fh = self._open(node.id)
                self.wp.write(node.id, fh, length-1, bytes(1))
                self.release(path, internal_fh)
                return 0

        # throw until there's an api for modifying existing files' length
        raise FuseOSError(errno.ENOSYS)

    def release(self, path, fh):
        """Releases an open ``path``."""

        if fh:
            node_id = self.fh_to_node[fh]
        else:
            node_id = self.cache.resolve_id(path)
        if not node_id:
            raise FuseOSError(errno.ENOENT)

        last_handle = False
        with self.fh_lock:
            """release the writer if there's no more interest. This allows many file
            handles to write to a single node provided they do it in order.
            """
            interest = self.node_to_fh.get(node_id)
            if interest:
                interest.discard(fh)
            if not interest:
                last_handle = True
                del self.node_to_fh[node_id]
            del self.fh_to_node[fh]

        if last_handle:
            self.rp.release(node_id)
            self.wp.release(node_id, None)
            self._xattr_flush(node_id)

        return 0

    def utimens(self, path, times=None):
        """Should set node atime and mtime to values as passed in ``times``
        or current time (see :manpage:`utimensat(2)`).
        Note that this is only implemented for modified time.

        :param path:
        :param times: [atime, mtime]"""

        node_id = self.cache.resolve_id(path)
        if not node_id:
            raise FuseOSError(errno.ENOENT)

        if times:
            # atime = times[0]
            mtime = times[1]
        else:
            # atime = time()
            mtime = time()

        try:
            self._setxattr(node_id, _XATTR_MTIME_OVERRIDE_NAME, mtime)
            self._xattr_flush(node_id)
        except:
            raise FuseOSError(errno.ENOTSUP)

        return 0

    def chmod(self, path, mode):
        node = self.cache.resolve(path)
        if not node:
            raise FuseOSError(errno.ENOENT)
        return self._chmod(node, mode)

    def _chmod(self, node, mode):
        mode_perms = stat.S_IMODE(mode)
        mode_type = stat.S_IFMT(self._getattr(node)['st_mode'])
        self._setxattr(node.id, _XATTR_MODE_OVERRIDE_NAME, mode_type | mode_perms)
        self._xattr_flush(node.id)
        return 0

    def chown(self, path, uid, gid):
        node_id = self.cache.resolve_id(path)
        if not node_id:
            raise FuseOSError(errno.ENOENT)
        return self._chown(node_id, uid, gid)

    def _chown(self, node_id, uid, gid):
        if uid != -1: self._setxattr(node_id, _XATTR_UID_OVERRIDE_NAME, uid)
        if gid != -1: self._setxattr(node_id, _XATTR_GID_OVERRIDE_NAME, gid)
        self._xattr_flush(node_id)
        return 0

    def symlink(self, target, source):
        source_bytes = source.encode('utf-8')
        fh = self.create(target, None)
        node_id = self.fh_to_node[fh]
        self._setxattr(node_id, _XATTR_MODE_OVERRIDE_NAME, stat.S_IFLNK | 0o0777)
        self.write(target, source_bytes, 0, fh)
        self.release(target, fh)
        return 0

    def readlink(self, path):
        node = self.cache.resolve(path)
        if not node:
            raise FuseOSError(errno.ENOENT)

        source = None

        if source is None:
            source_bytes = self.cache.get_content(node.id, node.version)
            if source_bytes is not None:
                source = source_bytes.decode('utf-8')

        if source is None:
            size = self.wp.length(node.id, None)
            if size is None: size = node.size
            source_bytes = self.read(path, size, 0)
            source = source_bytes.decode('utf-8')
            self.cache.insert_content(node.id, node.version, source_bytes)
        return source


def mount(path: str, args: dict, **kwargs) -> 'Union[int, None]':
    """Fusermounts Amazon Cloud Drive to specified mountpoint.

    :param path:
    :raises: RuntimeError
    :param args: args to pass on to ACDFuse init
    :param kwargs: fuse mount options as described in :manpage:`fuse(8)`"""

    if not os.path.isdir(path):
        logger.critical('Mountpoint does not exist or already used.')
        return 1

    opts = dict(auto_cache=True, sync_read=True)
    if sys.platform.startswith('linux'):
        opts['big_writes'] = True

    if sys.platform != 'darwin' or kwargs['volname'] is None:
        del kwargs['volname']

    kwargs.update(opts)

    args['conf'] = get_conf(args['settings_path'], _SETTINGS_FILENAME, _def_conf)

    FUSE(ACDFuse(**args), path, subtype=ACDFuse.__name__, **kwargs)


def unmount(path=None, lazy=False) -> int:
    """Unmounts a specific mountpoint if path given or all of the user's ACDFuse mounts.

    :returns: 0 on success, 1 on error"""

    import platform
    import re
    import subprocess

    system = platform.system().lower()

    if system != 'darwin':
        umount_cmd = ['fusermount', '-u']
    else:
        umount_cmd = ['umount']
    if lazy:
        if system == 'linux':
            umount_cmd.append('-z')
        else:
            logging.warning('Lazy unmounting is not supported on your platform.')

    fuse_st = ACDFuse.__name__

    if path:
        paths = [path]
    else:
        if system not in ['linux', 'darwin']:
            logger.critical('Automatic unmounting is not supported on your platform.')
            return 1

        paths = []
        try:
            if system == 'linux':
                mounts = subprocess.check_output(['mount', '-t', 'fuse.' + fuse_st])
            elif system == 'darwin':
                mounts = subprocess.check_output(['mount'])

            mounts = mounts.decode('UTF-8').splitlines()
        except:
            logger.critical('Getting mountpoints failed.')
            return 1

        for mount in mounts:
            if fuse_st in mount:
                if (system == 'linux' and 'user_id=%i' % os.getuid() in mount) or \
                (system == 'darwin' and 'mounted by %s' % os.getlogin() in mount):
                    paths.append(re.search(fuse_st + ' on (.*?) ', mount).group(1))

    ret = 0
    for path in paths:
        command = list(umount_cmd)
        command.append(path)
        try:
            subprocess.check_call(command)
        except subprocess.CalledProcessError:
            # logger.error('Unmounting %s failed.' % path)
            ret |= 1

    return ret
