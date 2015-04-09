from models import Experiment, Alternative, Client
from config import CONFIG as cfg


def participate(experiment, alternatives, client_id,
    force=None,
    forced_participate=None,
    traffic_fraction=None,
    prefetch=False,
    datetime=None,
    redis=None,
    queue=None):

    exp = Experiment.find_or_create(experiment, alternatives, traffic_fraction=traffic_fraction, redis=redis, queue=queue)

    alt = None
    if force and force in alternatives:
        alt = Alternative(force, exp, redis=redis)
    elif not cfg.get('enabled', True):
        alt = exp.control
    elif exp.winner is not None:
        alt = exp.winner
    elif forced_participate and forced_participate in alternatives:
        alt = Alternative(forced_participate, exp, redis=redis)
        client = Client(client_id, redis=redis)
        alt.record_participation(client, dt=datetime)
        exp.notify_queue_participation(client, alt)
    else:
        client = Client(client_id, redis=redis)
        alt = exp.get_alternative(client, dt=datetime, prefetch=prefetch)

    return alt


def convert(experiment, client_id,
    kpi=None,
    datetime=None,
    redis=None,
    queue=None):

    exp = Experiment.find(experiment, redis=redis, queue=queue)

    if cfg.get('enabled', True):
        client = Client(client_id, redis=redis)
        alt = exp.convert(client, dt=datetime, kpi=kpi)
    else:
        alt = exp.control

    return alt
