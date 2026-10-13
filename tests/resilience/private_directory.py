"""Owned qualification directory permissions; no account administration."""
import os
from pathlib import Path
import stat
import sys
from bounded_capture import capture

SCRIPT = r'''
$ErrorActionPreference = 'Stop'
$path = $args[0]
$mode = $args[1]
$stage = 'identity'
try {
$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$directory = [System.IO.DirectoryInfo]::new($path)
$stage = 'verify_existing_owner'
$existingOwner = $directory.GetAccessControl([System.Security.AccessControl.AccessControlSections]::Owner)
if ($existingOwner.GetOwner([System.Security.Principal.SecurityIdentifier]).Value -ne $identity.Value) { throw 'owned existing owner' }
if ($mode -eq 'secure') {
  $stage = 'access_descriptor'
  $acl = $directory.GetAccessControl([System.Security.AccessControl.AccessControlSections]::Access)
  $stage = 'protect_and_clear'
  $acl.SetAccessRuleProtection($true, $false)
  foreach ($oldRule in @($acl.GetAccessRules($true, $false, [System.Security.Principal.SecurityIdentifier]))) { $acl.RemoveAccessRuleSpecific($oldRule) }
  $stage = 'access_rule'
  $rule = [System.Security.AccessControl.FileSystemAccessRule]::new($identity, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
  $acl.AddAccessRule($rule)
  $stage = 'apply'
  $directory.SetAccessControl($acl)
}
$stage = 'reread'
$sections = [System.Security.AccessControl.AccessControlSections]([System.Security.AccessControl.AccessControlSections]::Access -bor [System.Security.AccessControl.AccessControlSections]::Owner)
$actual = $directory.GetAccessControl($sections)
$rules = @($actual.GetAccessRules($true, $true, [System.Security.Principal.SecurityIdentifier]))
$stage = 'verify_identity'
if ($actual.GetOwner([System.Security.Principal.SecurityIdentifier]).Value -ne $identity.Value -or $rules.Count -ne 1) { throw 'owned ACL identity' }
$rule = $rules[0]
$stage = 'verify_rights'
if ($rule.IdentityReference.Value -ne $identity.Value -or $rule.AccessControlType -ne 'Allow' -or $rule.FileSystemRights -ne 'FullControl' -or $rule.InheritanceFlags -ne 'ContainerInherit,ObjectInherit' -or $rule.PropagationFlags -ne 'None') { throw 'owned ACL rights' }
$stage = 'verify_protection'
if ($mode -eq 'secure' -and -not $actual.AreAccessRulesProtected) { throw 'owned ACL protection' }
} catch {
  $kind = $_.Exception.GetType().Name
  if ($kind.Length -gt 128) { $kind = $kind.Substring(0, 128) }
  $innerCode = 0
  if ($_.Exception.InnerException -ne $null) { $innerCode = $_.Exception.InnerException.HResult }
  [Console]::Error.WriteLine('owned_acl:' + $stage + ':' + $kind + ':' + $_.Exception.HResult + ':' + $innerCode)
  exit 1
}
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
            returncode, stdout, stderr = capture(['powershell.exe', '-NoProfile', '-NonInteractive',
                '-ExecutionPolicy', 'Bypass', '-File', str(script), str(path), 'secure' if secure else 'verify'],
                cwd=path, env=dict(os.environ), seconds=10, limit=4096)
            if returncode != 0 or stdout or stderr:
                error = ValueError('owned private ACL verification')
                try:
                    with (path / 'owned-acl-failed.log').open('xb') as stream:
                        stream.write(stderr)
                except BaseException:pass
                raise error
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
