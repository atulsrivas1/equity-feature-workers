"""Owned qualification directory permissions; no account administration."""
import os
from pathlib import Path
import stat
import subprocess
import sys

SCRIPT = r'''
$ErrorActionPreference = 'Stop'
$path = $args[0]
$mode = $args[1]
$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
if ($mode -eq 'secure') {
  $acl = [System.Security.AccessControl.DirectorySecurity]::new()
  $acl.SetAccessRuleProtection($true, $false)
  $acl.SetOwner($identity)
  $rule = [System.Security.AccessControl.FileSystemAccessRule]::new($identity, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
  $acl.AddAccessRule($rule)
  Set-Acl -LiteralPath $path -AclObject $acl
}
$actual = Get-Acl -LiteralPath $path
$rules = @($actual.GetAccessRules($true, $true, [System.Security.Principal.SecurityIdentifier]))
if ($actual.GetOwner([System.Security.Principal.SecurityIdentifier]).Value -ne $identity.Value -or $rules.Count -ne 1) { throw 'owned ACL identity' }
$rule = $rules[0]
if ($rule.IdentityReference.Value -ne $identity.Value -or $rule.AccessControlType -ne 'Allow' -or $rule.FileSystemRights -ne 'FullControl' -or $rule.InheritanceFlags -ne 'ContainerInherit,ObjectInherit' -or $rule.PropagationFlags -ne 'None') { throw 'owned ACL rights' }
if ($mode -eq 'secure' -and -not $actual.AreAccessRulesProtected) { throw 'owned ACL protection' }
'''


def private_directory(directory, *, secure=False):
    path = Path(directory)
    if not path.is_absolute() or path.is_symlink() or not path.is_dir() or path.resolve() != path:
        raise ValueError('owned private directory')
    if os.name == 'nt':
        # A fixed script file avoids command-string quoting of a filesystem path.
        script = path / 'owned-acl-check.ps1'
        if script.exists():raise ValueError('owned ACL script collision')
        try:
            with script.open('x', encoding='utf-8') as stream:stream.write(SCRIPT)
            result = subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive',
                '-ExecutionPolicy', 'Bypass', '-File', str(script), str(path), 'secure' if secure else 'verify'],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
            if result.returncode != 0:raise ValueError('owned private ACL verification')
        finally:
            original = sys.exc_info()[0] is not None
            try:
                if script.exists():script.unlink()
            except BaseException:
                if not original:raise
    else:
        if secure:path.chmod(0o700)
        observed = path.stat()
        if observed.st_uid != os.getuid() or stat.S_IMODE(observed.st_mode) != 0o700:
            raise ValueError('owned private POSIX permissions')
