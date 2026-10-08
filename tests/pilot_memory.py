"""External sampled RSS observations; no calculation or hard-limit enforcement."""
from __future__ import annotations
import ctypes
import os
from pathlib import Path
import threading
import time


def quantile(values, probability):
    if not values or not 0 <= probability <= 1:
        raise ValueError('quantile requires a population and probability')
    ordered = sorted(values)
    position = probability * (len(ordered) - 1)
    lower = int(position)
    return ordered[lower] + (ordered[min(lower + 1, len(ordered) - 1)] - ordered[lower]) * (position - lower)


def summary(values):
    return dict(count=len(values), minimum=min(values), p50=quantile(values, .5),
                p95=quantile(values, .95), maximum=max(values)) if values else None


def simultaneous_peak(frames):
    return max((sum(frame.values()) for frame in frames), default=0)


class NativeMemory:
    def __init__(self):
        if os.name != 'nt':
            return
        from ctypes import wintypes as w
        class Entry(ctypes.Structure):
            _fields_ = [('size', w.DWORD), ('usage', w.DWORD), ('pid', w.DWORD),
                        ('heap', ctypes.c_size_t), ('module', w.DWORD), ('threads', w.DWORD),
                        ('parent', w.DWORD), ('priority', w.LONG), ('flags', w.DWORD),
                        ('exe', w.WCHAR * 260)]
        class Counters(ctypes.Structure):
            _fields_ = [('size', w.DWORD), ('faults', w.DWORD)] + [
                (n, ctypes.c_size_t) for n in ('peak', 'rss', 'peak_paged', 'paged',
                'peak_nonpaged', 'nonpaged', 'pagefile', 'peak_pagefile', 'private')]
        self.Entry, self.Counters = Entry, Counters
        self.kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        self.psapi = ctypes.WinDLL('psapi', use_last_error=True)
        self.kernel.CreateToolhelp32Snapshot.argtypes = [w.DWORD, w.DWORD]
        self.kernel.CreateToolhelp32Snapshot.restype = w.HANDLE
        for name in ('Process32FirstW', 'Process32NextW'):
            fn = getattr(self.kernel, name); fn.argtypes = [w.HANDLE, ctypes.POINTER(Entry)]; fn.restype = w.BOOL
        self.kernel.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]
        self.kernel.OpenProcess.restype = w.HANDLE
        self.kernel.CloseHandle.argtypes = [w.HANDLE]; self.kernel.CloseHandle.restype = w.BOOL
        self.kernel.GetProcessTimes.argtypes = [w.HANDLE] + [ctypes.POINTER(w.FILETIME)] * 4
        self.kernel.GetProcessTimes.restype = w.BOOL
        self.psapi.GetProcessMemoryInfo.argtypes = [w.HANDLE, ctypes.POINTER(Counters), w.DWORD]
        self.psapi.GetProcessMemoryInfo.restype = w.BOOL

    def inventory(self):
        if os.name != 'nt':
            result = {}
            for directory in Path('/proc').iterdir():
                if not directory.name.isdecimal():
                    continue
                try:
                    parts = (directory / 'stat').read_text(encoding='ascii').rsplit(')', 1)[1].split()
                    result[int(directory.name)] = (int(parts[1]), int(parts[19]))
                except (OSError, ValueError, IndexError):
                    continue  # Inventory cannot observe processes disappearing between scans.
            return result
        handle = self.kernel.CreateToolhelp32Snapshot(2, 0)
        if handle == ctypes.c_void_p(-1).value:
            raise OSError('process inventory unavailable')
        result = {}
        try:
            entry = self.Entry(); entry.size = ctypes.sizeof(entry)
            present = self.kernel.Process32FirstW(handle, ctypes.byref(entry))
            while present:
                result[int(entry.pid)] = (int(entry.parent), None)
                present = self.kernel.Process32NextW(handle, ctypes.byref(entry))
            if ctypes.get_last_error() != 18:  # ERROR_NO_MORE_FILES
                raise OSError('process inventory incomplete')
            if not result:
                raise OSError('empty process inventory')
        finally:
            self.kernel.CloseHandle(handle)
        return result

    def read(self, pid, expected_creation=None):
        if os.name != 'nt':
            directory = Path('/proc') / str(pid)
            before = (directory / 'stat').read_text(encoding='ascii').rsplit(')', 1)[1].split()
            creation = int(before[19])
            fields = {}
            for line in (directory / 'status').read_text(encoding='ascii').splitlines():
                if line.startswith(('VmRSS:', 'VmHWM:')):
                    name, value, unit = line.split()
                    if unit != 'kB':
                        raise ValueError('memory units unavailable')
                    fields[name[:-1]] = int(value) * 1024
            after = (directory / 'stat').read_text(encoding='ascii').rsplit(')', 1)[1].split()
            if int(after[19]) != creation or expected_creation not in (None, creation):
                raise OSError('process identity changed')
            return creation, fields['VmRSS'], fields['VmHWM']
        from ctypes import wintypes as w
        handle = self.kernel.OpenProcess(0x410, False, pid)
        if not handle:
            raise OSError('process counters unavailable')
        try:
            times = [w.FILETIME() for _ in range(4)]
            if not self.kernel.GetProcessTimes(handle, *(ctypes.byref(t) for t in times)):
                raise OSError('process creation unavailable')
            creation = times[0].dwLowDateTime | (times[0].dwHighDateTime << 32)
            if expected_creation not in (None, creation):
                raise OSError('process identity changed')
            counters = self.Counters(); counters.size = ctypes.sizeof(counters)
            if not self.psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.size):
                raise OSError('process memory unavailable')
            return creation, int(counters.rss), int(counters.peak)
        finally:
            self.kernel.CloseHandle(handle)

    def frame(self, root):
        inventory = self.inventory(); members = {root}
        while True:
            added = {pid for pid, (parent, _) in inventory.items() if parent in members}
            if added <= members:
                break
            members.update(added)
        rows, errors = {}, []
        for pid in sorted(members):
            try:
                creation, rss, high = self.read(pid, inventory.get(pid, (None, None))[1])
                if rss <= 0:
                    raise OSError('resident counter unavailable')
                rows[f'{pid}:{creation}'] = dict(pid=pid, creation=creation, rss=rss, native_high_water=high)
            except (OSError, ValueError, KeyError, IndexError):
                errors.append(pid)
        return rows, errors


class MemorySampler:
    """Runs in the coordinator's external parent; its own RSS is excluded."""
    def __init__(self, process, interval=.01):
        self.process, self.interval = process, interval
        self.native = NativeMemory(); self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.scans, self.starts, self.peaks, self.peak_frame = [], [], {}, {}
        self.peak = 0; self.errors = 0; self.root_samples = 0; self.inventory_errors = 0
        self.root_creation = None; self.truncated = False
        self.fatal_error = False

    def start(self):
        self.thread.start(); return self

    def _run(self):
        while not self.stop_event.is_set() and self.process.poll() is None:
            start = time.perf_counter_ns()
            if len(self.starts) >= 20000:
                self.truncated = True; return
            try:
                frame, errors = self.native.frame(self.process.pid)
                root = next((v for v in frame.values() if v['pid'] == self.process.pid), None)
                if root:
                    if self.root_creation is None:
                        self.root_creation = root['creation']
                    if self.root_creation != root['creation']:
                        self.inventory_errors += 1; return
                    self.root_samples += 1
                self.errors += len(errors)
                total = sum(v['rss'] for v in frame.values())
                if total > self.peak:
                    self.peak, self.peak_frame = total, {k: v['rss'] for k, v in frame.items()}
                for key, value in frame.items():
                    if len(self.peaks) >= 256 and key not in self.peaks:
                        self.truncated = True; return
                    previous = self.peaks.setdefault(key, dict(pid=value['pid'], creation=value['creation'],
                        sampled_peak=0, native_high_water=0, samples=0))
                    previous['sampled_peak'] = max(previous['sampled_peak'], value['rss'])
                    previous['native_high_water'] = max(previous['native_high_water'], value['native_high_water'])
                    previous['samples'] += 1
            except (OSError, ValueError, KeyError, IndexError):
                self.inventory_errors += 1
            except Exception:
                self.fatal_error = True; return
            self.starts.append(start); self.scans.append(time.perf_counter_ns() - start)
            self.stop_event.wait(max(0, self.interval - self.scans[-1] / 1e9))

    def stop(self):
        self.stop_event.set(); self.thread.join(5)
        if self.thread.is_alive() or not self.root_samples or not self.peak or self.truncated or self.fatal_error:
            raise RuntimeError('sampled memory qualification unavailable')
        return dict(method='native working set' if os.name == 'nt' else '/proc VmRSS',
            nominal_interval_ns=int(self.interval * 1e9), root_samples=self.root_samples,
            frames=len(self.starts), intervals_ns=summary([b-a for a,b in zip(self.starts,self.starts[1:])]),
            scan_duration_ns=summary(self.scans), counter_unavailable=self.errors,
            inventory_unavailable=self.inventory_errors, sampled_job_peak_bytes=self.peak,
            peak_frame_bytes=self.peak_frame, processes=self.peaks,
            limitations='Sequential scan windows; shared pages double counted; short peaks/children and ancestry races may be missed. Unavailable counters are not zero. Native per-process high waters are not summed. External sampler overhead remains possible; no hard RSS cap.')


def hardware():
    """Public hardware facts only; never node/user/path or command-line inventory."""
    import platform
    import subprocess
    import json
    capacity = os.cpu_count() or 1; affinity = None; total = available = None
    model = platform.processor()
    if os.name == 'nt':
        from ctypes import wintypes as w
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        class Memory(ctypes.Structure):
            _fields_ = [('length', w.DWORD), ('load', w.DWORD)] + [
                (n, ctypes.c_ulonglong) for n in ('total', 'available', 'total_pagefile',
                    'available_pagefile', 'total_virtual', 'available_virtual', 'extended')]
        record = Memory(); record.length = ctypes.sizeof(record)
        kernel.GlobalMemoryStatusEx.argtypes = [ctypes.POINTER(Memory)]
        kernel.GlobalMemoryStatusEx.restype = w.BOOL
        if kernel.GlobalMemoryStatusEx(ctypes.byref(record)):
            total, available = int(record.total), int(record.available)
        kernel.GetCurrentProcess.restype = w.HANDLE
        kernel.GetProcessAffinityMask.argtypes = [w.HANDLE, ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_size_t)]
        kernel.GetProcessAffinityMask.restype = w.BOOL
        process_mask, system_mask = ctypes.c_size_t(), ctypes.c_size_t()
        if kernel.GetProcessAffinityMask(kernel.GetCurrentProcess(), ctypes.byref(process_mask), ctypes.byref(system_mask)):
            affinity = int(process_mask.value).bit_count() or None
        try:
            raw = subprocess.check_output(['powershell', '-NoProfile', '-Command',
                'Get-CimInstance Win32_Processor | Select-Object -ExpandProperty Name | ConvertTo-Json'],
                encoding='utf-8', timeout=15, creationflags=subprocess.CREATE_NO_WINDOW)
            model = json.loads(raw)
        except (OSError, subprocess.SubprocessError, ValueError):
            pass
    else:
        if hasattr(os, 'sched_getaffinity'):
            affinity = len(os.sched_getaffinity(0))
        try:
            fields = {line.split(':')[0]: int(line.split()[1]) * 1024
                for line in Path('/proc/meminfo').read_text(encoding='ascii').splitlines()
                if line.startswith(('MemTotal:', 'MemAvailable:'))}
            total, available = fields['MemTotal'], fields['MemAvailable']
            model = next(line.split(':',1)[1].strip() for line in Path('/proc/cpuinfo').read_text(encoding='ascii').splitlines()
                if line.startswith('model name'))
        except (OSError, ValueError, KeyError, StopIteration):
            pass
    return dict(system=platform.system(), release=platform.release(), machine=platform.machine(),
                python=platform.python_version(), cpu_model=model, logical_cpus=capacity,
                affinity_cpus=affinity, admitted_cpu_capacity=min(capacity, affinity or capacity),
                total_physical_bytes=total, available_physical_bytes=available)


def admission(facts, workers):
    if workers + 1 > facts['admitted_cpu_capacity']:
        return 'workers plus one backend/coordinator slot exceed observed affinity/logical capacity'
    available = facts['available_physical_bytes']
    if available is None:
        return 'physical RAM observation unavailable; admission unavailable'
    if available < 1073741824 + workers * 268435456:
        return 'observed physical RAM below predeclared 1GiB plus workers*256MiB heuristic'
    return None
