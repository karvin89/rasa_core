from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import itertools
import typing

import json
import logging

# noinspection PyPep8Naming
import six.moves.cPickle as pickler
from typing import Text, Optional, List
from requests_futures.sessions import FuturesSession
import jsonpickle

from rasa_core.actions.action import ACTION_LISTEN_NAME
from rasa_core.broker import EventChannel
from rasa_core.trackers import (
    DialogueStateTracker, ActionExecuted,
    EventVerbosity)

logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from rasa_core.domain import Domain


class TrackerStore(object):
    def __init__(self, domain, event_broker=None, publish_url=None):
        # type: (Optional[Domain], Optional[EventChannel]) -> None
        self.domain = domain
        self.event_broker = event_broker
        self.publish_url = publish_url

    def get_or_create_tracker(self, sender_id):
        tracker = self.retrieve(sender_id)
        if tracker is None:
            tracker = self.create_tracker(sender_id)
        return tracker

    def init_tracker(self, sender_id):
        if self.domain:
            return DialogueStateTracker(sender_id,
                                        self.domain.slots)
        else:
            return None

    def create_tracker(self, sender_id, append_action_listen=True):
        """Creates a new tracker for the sender_id.

        The tracker is initially listening."""

        tracker = self.init_tracker(sender_id)
        if tracker:
            if append_action_listen:
                tracker.update(ActionExecuted(ACTION_LISTEN_NAME))
            self.save(tracker)
        return tracker

    def save(self, tracker):
        raise NotImplementedError()

    def retrieve(self, sender_id):
        # type: (Text) -> Optional[DialogueStateTracker]
        raise NotImplementedError()

    def stream_events(self, tracker):
        # type: (DialogueStateTracker) -> None
        old_tracker = self.retrieve(tracker.sender_id)
        offset = len(old_tracker.events) if old_tracker else 0
        evts = tracker.events
        for evt in list(itertools.islice(evts, offset, len(evts))):
            body = {
                "sender_id": tracker.sender_id,
            }
            body.update(evt.as_dict())
            self.event_broker.publish(json.dumps(body))

    def keys(self):
        # type: () -> Optional[List[Text]]
        raise NotImplementedError()

    def post_tracker(self, tracker):
        if(self.publish_url):
            headers = {'Content-type': 'application/json', 
                        'Accept': 'application/json'}
            with FuturesSession() as session:
                future = session.post(self.publish_url,
                json=tracker.current_state(event_verbosity=EventVerbosity.ALL), 
                headers=headers)

    @staticmethod
    def serialise_tracker(tracker):
        dialogue = tracker.as_dialogue()
        return pickler.dumps(dialogue)

    def serialise_tracker_as_jsonstring(self, tracker):
        dialogue = tracker.as_dialogue()
        return jsonpickle.encode(dialogue)

    def deserialise_tracker(self, sender_id, _json):
        dialogue = pickler.loads(_json)
        tracker = self.init_tracker(sender_id)
        tracker.recreate_from_dialogue(dialogue)
        return tracker


class InMemoryTrackerStore(TrackerStore):
    def __init__(self, domain, event_broker=None, publish_url=None):
        self.store = {}
        super(InMemoryTrackerStore, self).__init__(domain, event_broker, publish_url)

    def save(self, tracker):
        if self.event_broker:
            self.stream_events(tracker)
        serialised = InMemoryTrackerStore.serialise_tracker(tracker)
        self.store[tracker.sender_id] = serialised
        self.post_tracker(tracker)

    def retrieve(self, sender_id):
        if sender_id in self.store:
            logger.debug('Recreating tracker for '
                         'id \'{}\''.format(sender_id))
            return self.deserialise_tracker(sender_id, self.store[sender_id])
        else:
            logger.debug('Creating a new tracker for '
                         'id \'{}\'.'.format(sender_id))
            return None

    def keys(self):
        return self.store.keys()


class RedisTrackerStore(TrackerStore):
    def keys(self):
        pass

    def __init__(self, domain, host='localhost',
                 port=6379, db=0, password=None, event_broker=None, publish_url=None):

        import redis
        self.red = redis.StrictRedis(host=host, port=port, db=db,
                                     password=password)
        super(RedisTrackerStore, self).__init__(domain, event_broker, publish_url)

    def save(self, tracker, timeout=None):
        if self.event_broker:
            self.stream_events(tracker)

        serialised_tracker = self.serialise_tracker(tracker)
        self.red.set(tracker.sender_id, serialised_tracker, ex=timeout)
        self.post_tracker(tracker)

    def retrieve(self, sender_id):
        stored = self.red.get(sender_id)
        if stored is not None:
            return self.deserialise_tracker(sender_id, stored)
        else:
            return None


class MongoTrackerStore(TrackerStore):
    def __init__(self,
                 domain,
                 host="mongodb://localhost:27017",
                 db="rasa",
                 username=None,
                 password=None,
                 collection="conversations",
                 event_broker=None):
        from pymongo.database import Database
        from pymongo import MongoClient

        self.client = MongoClient(host,
                                  username=username,
                                  password=password,
                                  # delay connect until process forking is done
                                  connect=False)

        self.db = Database(self.client, db)
        self.collection = collection
        super(MongoTrackerStore, self).__init__(domain, event_broker)

        self._ensure_indices()

    @property
    def conversations(self):
        return self.db[self.collection]

    def _ensure_indices(self):
        self.conversations.create_index("sender_id")

    def save(self, tracker, timeout=None):
        if self.event_broker:
            self.stream_events(tracker)

        state = tracker.current_state(EventVerbosity.ALL)

        self.conversations.update_one(
                {"sender_id": tracker.sender_id},
                {"$set": state},
                upsert=True)

    def retrieve(self, sender_id):
        stored = self.conversations.find_one({"sender_id": sender_id})
        if stored is not None:
            if self.domain:
                return DialogueStateTracker.from_dict(sender_id,
                                                      stored.get("events"),
                                                      self.domain.slots)
            else:
                logger.warning("Can't recreate tracker from mongo storage "
                               "because no domain is set. Returning `None` "
                               "instead.")
                return None
        else:
            return None

    def keys(self):
        return [c["sender_id"] for c in self.conversations.find()]
