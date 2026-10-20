"""Pure V8 source/capacity matrix joins; no probe, writer or study authority.

The controller must supply its actual original reference, capacity observation
and returned freeze reference. These checks cannot authenticate caller maps.
"""
from eq142_graph import wire,digest
from eq142_owned_runner import PROFILES,SERIES
from eq142_proofs import check_body
from eq142_capacity import admit_capacity

ORIGINAL=(('sequential',1),('thread',1),('thread',2),('thread',4),('thread',8),
          ('process',1),('process',2),('process',4),('process',8))
FOCUSED=(('sequential',1),('thread',2),('process',2))


def selectors():
    """Fixed full protocol order, including later unsupported entries."""
    return [{'profile':profile,'mode':mode,'degree':degree,'series':series}
            for index,profile in enumerate(PROFILES)
            for mode,degree in (ORIGINAL if index<3 else FOCUSED)
            for series in SERIES]


def build_matrix(reference,capacity,capacity_reference,*,probe_sources,
                 expected_device,expected_retained_bytes):
    """Bind complete original state/inventory and actual freeze bytes.

    Capacity/source authentication remains the controller's responsibility.
    Memory/disk/source failures are errors, never unsupported-cohort omissions.
    """
    check_body(wire(capacity),capacity_reference,scope='qualification',name='qualification-capacity.json')
    if capacity_reference['codec']!='capacity_receipt' or capacity['position']!='before':
        raise ValueError('matrix fixed capacity freeze')
    admit_capacity(capacity,probe_sources,degree=1,expected_device=expected_device,
                   expected_retained_bytes=expected_retained_bytes)
    if (type(reference) is not dict or reference.get('schema')!='eq142-development-reference:1'
            or reference.get('stage')!='qualification_only_not_frozen_study'
            or type(reference.get('state')) is not dict
            or set(reference['state'])!={'files','installed_runtime'}
            or type(reference.get('source_inventory')) is not dict
            or type(reference.get('cpu_slots')) is not int
            or reference['cpu_slots']!=capacity['allowed_cpu_slots']
            or type(reference.get('profiles')) is not dict
            or set(reference['profiles'])!=set(PROFILES)):
        raise ValueError('matrix original full reference context')
    rows=[]
    for index,selector in enumerate(selectors()):
        supported=selector['degree']+3<=capacity['allowed_cpu_slots']
        rows.append({'cohort_id':f'c{index:03d}','selector':selector,'supported':supported,
                     'omission_reason':None if supported else 'cpu_primary_group_slots_insufficient'})
    return {'schema':'eq142-source-capacity-matrix:1',
            'source_state_sha256':digest(wire(reference['state'])),
            'source_inventory_sha256':digest(wire(reference['source_inventory'])),
            'capacity_freeze_reference':dict(capacity_reference),'cohorts':rows}


def verify_matrix(record,reference,capacity,capacity_reference,**context):
    expected=build_matrix(reference,capacity,capacity_reference,**context)
    if wire(record)!=wire(expected):raise ValueError('matrix complete source/capacity/order binding')
    return {'matrix_sha256':digest(wire(expected)),
            'supported_cohort_ids':[row['cohort_id'] for row in expected['cohorts'] if row['supported']],
            'scope':'pure_matrix_join_not_capacity_or_source_authority','full_sample_accepted':False}
