"""跨进程安全的文件读写原语（2026-09-25）。

为什么需要这个模块
------------------
温层的写有两条路：

* `extract_warm_from_date` —— 追加新提炼的记忆（一次一行）；
* `update_warm_entry_call_count` —— 读全文 → 改一行 → **整文件重写**。

原先第二条路直接 `open(path, "w")` 然后 `writelines()`：写一半进程被杀
（任务计划、Ctrl-C、MCP 子进程被 dsh 重启）就把当天温层文件截断 ——
jsonl 一行一条记忆，截断即丢记忆，且**丢的那些条没有任何报错**。

而且两条路之间没有任何锁，只有一个进程内的 `threading.Lock`，
所以「另一个进程正在整文件重写」与「本进程正在追加」会互相盖掉。

这里把两件事做成可复用的原语：

* `atomic_write_text()` —— 写同目录临时文件 + `os.replace()` 原子替换，
  读者永远看到完整的旧版或完整的新版，不存在"写了一半"的中间态；
* `file_lock()` —— 内核级文件锁，`threading.Lock` 覆盖不到的
  **另一进程 / 另一解释器实例**也互斥。
"""

import contextlib
import errno
import os
import tempfile
import time

_IS_WINDOWS = os.name == "nt"

if _IS_WINDOWS:
    import msvcrt

    def _try_lock(fd):
        # LK_NBLCK 在锁被占用时不会立刻抛错，而是每秒重试一次、10 次后
        # 才抛 OSError —— 这是 Windows 的既定行为，所以 Windows 上拿到锁的
        # 等待粒度是 ~1 秒，`timeout` 只能粗控（不影响正确性）。
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)

    def _unlock(fd):
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
else:
    import fcntl

    def _try_lock(fd):
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(fd):
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass


@contextlib.contextmanager
def file_lock(target, timeout=20.0, poll=0.05):
    """对 `target` 加一把跨进程排他锁（锁文件是 `<target>.lock`）。

    锁文件**只创建不删除**：删掉它会引入"删的瞬间另一进程正好在建"的竞态，
    而锁本身由操作系统在进程退出时自动释放，不需要靠删文件来解锁。

    Args:
        target: 被保护的文件路径（锁名由它派生）。
        timeout: 等锁上限（秒）。超时抛 TimeoutError，让调用方知道
            「有人持有锁太久」，而不是静默地无锁写下去。
        poll: 重试间隔（秒）。
    """
    lock_path = target + ".lock"
    parent = os.path.dirname(os.path.abspath(lock_path))
    os.makedirs(parent, exist_ok=True)
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        # msvcrt 锁的是"从当前位置起 N 字节"，空文件先放一个字节保证区间存在。
        if os.fstat(fd).st_size == 0:
            os.write(fd, b"\0")
        deadline = time.monotonic() + timeout
        while True:
            try:
                _try_lock(fd)
                break
            except OSError as exc:
                if exc.errno not in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                    raise
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"等待文件锁超时（{timeout}s）：{lock_path}")
                time.sleep(poll)
        try:
            yield
        finally:
            _unlock(fd)
    finally:
        os.close(fd)


def atomic_write_text(path, text, encoding="utf-8"):
    """原子地整文件写入：临时文件 → fsync → `os.replace()`。

    `os.replace()` 在同一卷上是原子的（Windows/POSIX 皆然），所以：
    * 写的过程中进程被杀 → 目标文件仍是**完整的旧版**（临时文件残留，无害）；
    * 不会出现"读到一半的新版"。
    """
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix="-" + os.path.basename(path))
    try:
        with os.fdopen(fd, "w", encoding=encoding, newline="") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise