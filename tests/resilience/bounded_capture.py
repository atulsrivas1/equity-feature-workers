"""Bound actual qualification parent output while it is produced."""
import subprocess
import threading
import time
import sys


class CaptureFailure(ValueError):
    def __init__(self, reason, buffers):
        super().__init__('owned capture failed; cleanup unconfirmed: ' + reason)
        self.stdout, self.stderr = (bytes(value) for value in buffers)


def capture(argv, *, cwd, env, seconds, limit=262144):
    if type(seconds) is not int or not 0 < seconds <= 180 or type(limit) is not int or not 0 < limit <= 262144:
        raise ValueError('owned capture bounds')
    buffers = [bytearray(), bytearray()]
    failure = threading.Event()
    readers = [None, None]
    def read(pipe, index):
        try:
            while True:
                block = pipe.read(4096)
                if not block:return
                remaining = limit - len(buffers[index])
                buffers[index].extend(block[:remaining])
                if len(block) > remaining:
                    failure.set();return
        except BaseException:
            failure.set()
    # Allocate all reader bookkeeping before launching any owned process.
    process = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
    try:
        for index, pipe in enumerate((process.stdout, process.stderr)):
            thread = threading.Thread(target=read, args=(pipe, index), daemon=True)
            readers[index] = thread
            thread.start()
        deadline = time.monotonic() + seconds
        while True:
            if failure.is_set():raise CaptureFailure('output_or_reader', buffers)
            remaining = deadline - time.monotonic()
            if remaining <= 0:raise CaptureFailure('timeout', buffers)
            try:
                process.wait(timeout=min(.1, remaining));break
            except subprocess.TimeoutExpired:pass
        for thread in readers:thread.join(2)
        if any(thread.is_alive() for thread in readers) or failure.is_set():
            raise CaptureFailure('reader_completion', buffers)
        return process.returncode, bytes(buffers[0]), bytes(buffers[1])
    finally:
        original = sys.exc_info()[0] is not None
        cleanup_error = None
        try:
            live = process.poll() is None
        except BaseException as error:
            cleanup_error = error
            live = True
        if live:
            try:process.kill()
            except BaseException as error:
                if cleanup_error is None:cleanup_error = error
        try:
            process.wait(timeout=3)
        except BaseException as error:
            if cleanup_error is None:cleanup_error = error
        for pipe in (process.stdout, process.stderr):
            try:
                pipe.close()
                if pipe.closed is not True:raise ValueError('owned pipe close unconfirmed')
            except BaseException as error:
                if cleanup_error is None:cleanup_error = error
        for thread in readers:
            if thread is None:continue
            try:
                thread.join(2)
                if thread.is_alive() and cleanup_error is None:
                    cleanup_error = ValueError('owned reader still live')
            except BaseException as error:
                if cleanup_error is None:cleanup_error = error
        # Failure never claims descendant/scheduler cleanup from parent kill or return code.
        if cleanup_error is not None and not original:raise ValueError('owned capture cleanup unconfirmed') from cleanup_error
