"""Development namespace component; not a complete-sample certificate.

The controller must independently establish actual writer/source/namespace and
full original proof authority. No study or performance acceptance is implied.
"""
from dataclasses import dataclass
from functools import wraps
import os
import re
from pathlib import Path
from types import MappingProxyType
from eq142_proofs import validate_reference,read_proof

GIB=1073741824
CATEGORY_MAX=MappingProxyType({'regular.trace':43486543872,'regular.native':21743271936,
    'regular.summary':1358954496,'regular.diagnostics':1358954496,'shared.products':268435456,
    'regular.metadata':1073741824,'qualification':6*GIB})


def same_members(directory,files,subdirectories,*,maximum):
    """Bounded streaming directory scan; reject indirections and unknown names."""
    if type(maximum) is not int or maximum<1:raise ValueError('namespace scan bound')
    directory=Path(directory).absolute()
    for item in (directory,*directory.parents):
        if item.is_symlink() or item.is_junction():raise ValueError('namespace indirection')
    directory=directory.resolve(strict=True)
    if not directory.is_dir():raise ValueError('namespace directory')
    expected=set(files)|set(subdirectories)
    if len(expected)!=len(files)+len(subdirectories) or len(expected)>maximum:
        raise ValueError('namespace expected bound')
    actual=set()
    with os.scandir(directory) as entries:
        for entry in entries:
            if len(actual)>=maximum or entry.name not in expected or entry.name in actual:
                raise ValueError('namespace unexpected member')
            actual.add(entry.name);path=directory/entry.name
            if entry.is_symlink() or path.is_junction():raise ValueError('namespace member indirection')
            if entry.name in files:
                if not entry.is_file(follow_symlinks=False):raise ValueError('namespace expected regular file')
            elif not entry.is_dir(follow_symlinks=False):raise ValueError('namespace expected directory')
    if actual!=expected:raise ValueError('namespace missing member')
    return directory


@dataclass(frozen=True)
class FixedMember:
    """Controller-created policy, never decoded from arbitrary producer paths."""
    scope:str
    category:str
    reference:dict


def fail_closed(method):
    @wraps(method)
    def wrapped(self,*args,**kwargs):
        if self._failed:raise ValueError('namespace prior failure retained; no reuse')
        try:return method(self,*args,**kwargs)
        except BaseException:
            self._failed=True
            raise
    return wrapped


class EncodedJournal:
    """Actual returned/read-back refs only; no preallocation or atomicity claim.

    The final controller owns namespace identities and tree shape. New bodies
    are registered after the original writer/external persistence actually
    returns. Reconciliation rereads every declared body against its original
    ref; same shared product is charged once. Paths stay process-local.
    """
    def __init__(self,study_directory):
        requested=Path(study_directory).absolute()
        for item in (requested,*requested.parents):
            if item.is_symlink() or item.is_junction():raise ValueError('namespace study indirection')
        self._root=requested.resolve(strict=True)
        if not self._root.is_dir():raise ValueError('namespace study directory')
        self._members={};self._directories={};self._failed=False

    @fail_closed
    def register(self,namespace_id,directory,members):
        if (type(namespace_id) is not str or not 1<=len(namespace_id)<=256
                or type(members) is not dict or len(members)>512):
            raise ValueError('namespace registration')
        directory=Path(directory).absolute()
        for item in (directory,*directory.parents):
            if item.is_symlink() or item.is_junction():raise ValueError('namespace registration indirection')
        directory=directory.resolve(strict=True)
        if not directory.is_relative_to(self._root):raise ValueError('namespace outside owned study')
        if namespace_id in self._directories and self._directories[namespace_id]!=directory:
            raise ValueError('namespace identity moved')
        if any(key!=namespace_id and value==directory for key,value in self._directories.items()):
            raise ValueError('namespace duplicate directory identity')
        self._directories[namespace_id]=directory
        for name,member in members.items():
            if (type(member) is not FixedMember or type(member.category) is not str
                    or type(member.scope) is not str or member.category not in CATEGORY_MAX):
                raise ValueError('namespace member policy')
            validate_reference(member.reference,scope=member.scope,name=name)
            key=(namespace_id,name)
            # Preserve previous original ref; never overwrite it with a rehash.
            if key in self._members and self._members[key]!=member:
                raise ValueError('namespace changed writer reference')
            read_proof(directory,member.reference,scope=member.scope,name=name)
            self._members[key]=FixedMember(member.scope,member.category,dict(member.reference))
        self.totals()

    @fail_closed
    def totals(self):
        totals={category:0 for category in CATEGORY_MAX}
        for member in self._members.values():totals[member.category]+=member.reference['bytes']
        if any(totals[key]>maximum for key,maximum in CATEGORY_MAX.items()):
            raise ValueError('namespace category encoded cap')
        regular=sum(value for key,value in totals.items() if key!='qualification')
        if regular>69289902080 or sum(totals.values())>72*GIB:
            raise ValueError('namespace study encoded cap')
        return {'categories':totals,'total_bytes':sum(totals.values()),
            'remaining_study_allowance_bytes':72*GIB-sum(totals.values()),
            'physical_allocation_guaranteed':False,'atomic_snapshot':False}

    @fail_closed
    def reconcile(self,children):
        """children maps every registered namespace ID to fixed immediate dirs.

        Each child directory must itself have a registered namespace, including
        intentionally empty fixed directories. Parent/root tree relationships
        are established by the controller, not arbitrary JSON pointer selectors.
        """
        if type(children) is not dict or set(children)!=set(self._directories):
            raise ValueError('namespace directory policy population')
        roots=[key for key,directory in self._directories.items() if directory==self._root]
        if len(roots)!=1:raise ValueError('namespace study root missing')
        for namespace_id,directory in self._directories.items():
            files={name:member for (scope,name),member in self._members.items() if scope==namespace_id}
            subdirectories=children[namespace_id]
            if type(subdirectories) is not dict or len(subdirectories)>2048:
                raise ValueError('namespace child policy')
            for name,child_id in subdirectories.items():
                if (type(name) is not str or re.fullmatch('[a-z][a-z0-9_.-]{0,127}',name) is None
                        or child_id not in self._directories
                        or self._directories[child_id]!=(directory/name).resolve(strict=True)):
                    raise ValueError('namespace fixed child differs')
            same_members(directory,files,subdirectories,maximum=max(1,len(files)+len(subdirectories)))
            for name,member in files.items():
                read_proof(directory,member.reference,scope=member.scope,name=name)
            same_members(directory,files,subdirectories,maximum=max(1,len(files)+len(subdirectories)))
        visited=set();pending=roots[:]
        while pending:
            current=pending.pop()
            if current in visited:raise ValueError('namespace repeated tree child')
            visited.add(current);pending.extend(children[current].values())
        if visited!=set(self._directories):raise ValueError('namespace disconnected tree')
        return self.totals()
