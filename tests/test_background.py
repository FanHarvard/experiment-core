import pytest

from experiment_core.background import BackgroundCoordinator
from experiment_core.errors import Conflict, StaleContext


def coordinator():
    now=[10.]
    return BackgroundCoordinator(lambda:now[0]),now


def test_reservation_fences_and_cannot_release_an_unconfirmed_return():
    bg,now=coordinator()
    epoch=bg.begin(0,{'owner':'one'},{'current':1.},5.,{'monitor':'one'})
    bg.reserve(['shot'])
    bg.reserve(['shot'])
    assert bg.generation==1
    with pytest.raises(StaleContext):bg.validate(epoch['token'],{'owner':'one'})
    bg.release(['shot'])
    with pytest.raises(Conflict):bg.begin(1,{}, {},5.,{})
    assert not bg.status()['foreground_ready']
    with pytest.raises(Conflict):bg.complete(epoch['token'],{'owner':'one'},{'confirmed':True})
    bg.complete(epoch['token'],{'owner':'one'},{'confirmed':True,'response_ended':True})
    assert bg.status()['foreground_ready']


def test_deadline_blocks_handoff_instead_of_replaying_or_claiming_ready():
    bg,now=coordinator()
    epoch=bg.begin(0,{}, {},2.,{})
    bg.returning('cancelled')
    now[0]+=3
    with pytest.raises(Conflict):bg.validate(epoch['token'],{},restoring=True)
    assert bg.status()['epoch']['state']=='reconciliation_required'
    assert not bg.status()['foreground_ready']


def test_adoption_is_atomic_with_generation_and_new_baseline():
    bg,now=coordinator()
    epoch=bg.begin(0,{}, {'current':1.},5.,{})
    bg.complete(epoch['token'],{},dict(confirmed=True,response_ended=True,current=2.),adopted=True)
    bg.reserve(['target'])
    assert bg.status()['last']['state']=='adopted'
    bg.release(['target'])
    next_epoch=bg.begin(1,{}, {'current':2.},5.,{})
    bg.reserve(['shot'])
    with pytest.raises(StaleContext):
        bg.complete(next_epoch['token'],{},dict(confirmed=True,response_ended=True),adopted=True)


def test_changed_context_and_old_token_never_authorize_restoration():
    bg,_=coordinator()
    epoch=bg.begin(0,{'policy':'one'}, {},5.,{})
    with pytest.raises(StaleContext):bg.validate('old',{'policy':'one'},restoring=True)
    with pytest.raises(StaleContext):bg.validate(epoch['token'],{'policy':'two'},restoring=True)
    assert bg.status()['epoch']['state']=='reconciliation_required'


def test_verified_handover_has_no_optical_deadline_but_still_requires_receipt():
    bg,now=coordinator()
    epoch=bg.begin(0,{'target':1},{'pi':1},None,{},completion_mode='verified_handover')
    bg.reserve(['target-change'])
    now[0]+=1000
    bg.validate(epoch['token'],{'target':1},restoring=True)
    assert 'return_deadline' not in bg.epoch and not bg.status()['foreground_ready']
    with pytest.raises(Conflict):bg.complete(epoch['token'],{'target':1},{'confirmed':False})
    bg.complete(epoch['token'],{'target':1},{'confirmed':True,'response_ended':True})
    assert bg.status()['foreground_ready']


@pytest.mark.parametrize('timeout,mode',[(20,'verified_handover'),(None,'deadline'),(None,'unknown')])
def test_handoff_contract_cannot_be_implicitly_weakened(timeout,mode):
    bg,_=coordinator()
    with pytest.raises(ValueError):bg.begin(0,{}, {},timeout,{},completion_mode=mode)
