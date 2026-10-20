"""Fixed V8 preparation callback collector; no standalone writer authority.

The controller owns the newly exclusive study and passes this exact callback to
the original prepare_reference. Arbitrary calls cannot prove original execution.
"""
from copy import deepcopy
import os
from pathlib import Path
from eq142_owned_runner import PROFILES,SERIES
from eq142_proofs import read_proof
from eq142_namespace import fail_closed


class PreparationRecorder:
    def __init__(self,reference_directory):
        self.root=Path(reference_directory).absolute()
        for path in (self.root,*self.root.parents):
            if path.is_symlink() or path.is_junction():raise ValueError('preparation indirection')
        if self.root.name!='reference' or not self.root.parent.is_dir() or os.path.lexists(self.root):
            raise ValueError('preparation requires fixed nonexistent reference directory')
        self.expected=tuple((profile,series) for profile in PROFILES for series in SERIES)
        self._entries=[];self._failed=False;self._finished=False

    @fail_closed
    def __call__(self,profile,series,products_reference,diagnostics_reference):
        if (self._finished or len(self._entries)>=len(self.expected)
                or (profile,series)!=self.expected[len(self._entries)]):
            raise ValueError('preparation callback fixed order/population')
        local=self.root/(profile+'-'+series)
        for ref,name,codec in ((products_reference,'products.json','eq142_product_projection'),
                               (diagnostics_reference,'diagnostics.json','diagnostic_snapshot')):
            if type(ref) is not dict or ref.get('codec')!=codec:
                raise ValueError('preparation fixed codec')
            read_proof(local,ref,scope='reference_preparation',name=name)
        self._entries.append({'profile':profile,'series':series,
                              'products_reference':deepcopy(products_reference),
                              'diagnostics_reference':deepcopy(diagnostics_reference)})

    @fail_closed
    def finish(self):
        if self._finished or len(self._entries)!=12:
            raise ValueError('preparation callbacks incomplete or already closed')
        # Reopen all original callback-selected bodies, including earlier pairs.
        for entry in self._entries:
            local=self.root/(entry['profile']+'-'+entry['series'])
            for key,name in (('products_reference','products.json'),('diagnostics_reference','diagnostics.json')):
                read_proof(local,entry[key],scope='reference_preparation',name=name)
        self._finished=True
        return deepcopy(self._entries)
