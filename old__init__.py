"""Radicale storage plugin for MongoDB."""

from base64 import b64encode
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta
from hashlib import sha256
from os.path import splitext

import vobject
from pymongo import MongoClient
from pytz import utc
from radicale.item import Item
from radicale.pathutils import strip_path, unstrip_path
from radicale.storage import BaseCollection, BaseStorage

VERSION = __version__ = "1.0.0"

# Keys available in the configuration file
PLUGIN_CONFIG_SCHEMA = {
    "storage": {
        "db_url": {"value": "mongodb://localhost/", "type": str},
        "db_name": {"value": "base", "type": str},
        "collection_name": {"value": "agenda", "type": str},
    },
}
# History of modifications, used for the sync-token
HISTORY = {}


def _mongo_datetime(key, value, vobject_event, mongo_event):
    """Read Mongo fields with datetimes and store them in Radicale item."""
    if not value:
        return

    keys = {
        "start": "dtstart",
        "end": "dtend",
        "createdAt": "created",
        "updatedAt": "last-modified",
    }
    value = value.replace(tzinfo=utc)
    if mongo_event.get("allDay") and key in ("start", "end"):
        value = value.astimezone().date()
    vobject_event.add(keys[key]).value = value


def _mongo_guests(key, value, vobject_event, mongo_event):
    """Read Mongo "guests" field and store it in Radicale item."""
    for guest in value or ():
        vobject_event.add("attendee").value = f"mailto:{guest}"


def _mongo_participants(key, value, vobject_event, mongo_event):
    """Read Mongo "participants" field and store it in Radicale item."""
    for participant in value or ():
        attendee = vobject_event.add("attendee")
        attendee.value = f'mailto:{participant["email"]}'
        if participant["status"] == 1:
            attendee.rsvp_param = "TRUE"
            attendee.partstat_param = "NEEDS-ACTION"
        else:
            attendee.rsvp_param = "FALSE"
            if participant["status"] == 0:
                attendee.partstat_param = "DECLINED"
            elif participant["status"] == 2:
                attendee.partstat_param = "ACCEPTED"


def _item_datetime(key, fields, vobject_event, mongo_event):
    """Read item fields with datetimes and store them in Mongo event."""
    keys = {
        "dtstart": "start",
        "dtend": "end",
        "created": "createdAt",
        "last-modified": "updatedAt",
    }

    value = datetime.now(utc) if key in ("created", "last-modified") else None
    if fields:
        (field,) = fields  # These fields don’t allow multiple values in iCal
        value = field.value

    if not value:
        mongo_event[keys[key]] = None
        return

    only_date = not isinstance(value, datetime)
    if key in ("dtstart", "dtend"):
        mongo_event["allDay"] = only_date
    if only_date:
        value = datetime(value.year, value.month, value.day)
    else:
        value = value
    mongo_event[keys[key]] = value.astimezone(tz=utc)


def _item_attendee(key, fields, vobject_event, mongo_event):
    """Read item "attendee" fields and store them in Mongo event."""
    # Keep original participants separately from guests
    participants_emails = {
        participant["email"]: participant for participant in mongo_event["participants"]
    }
    # Clear the participants and the guests stored in Mongo
    mongo_event["participants"] = []
    mongo_event["guests"] = []

    if not fields:
        return

    for field in fields:
        value = field.value
        if value.startswith("mailto:"):
            value = value[len("mailto:") :]
        if value in participants_emails:
            # Attendee was known as a participant, keep him
            participant = participants_emails[value]
            if field.partstat_param == "DECLINED":
                participant["status"] == 2
            elif field.partstat_param == "ACTEPTED":
                participant["status"] == 0
            elif field.rsvp_param == "FALSE":
                participant["status"] == 1
            mongo_event["participants"].append(participant)
        else:
            # Attendee was not a participant, add him to guests
            mongo_event["guests"].append(value)


MONGO_TO_ITEM = {
    "_id": "uid",
    "title": "summary",
    "description": "description",
    "location": "location",
    "guests": _mongo_guests,
    "participants": _mongo_participants,
    "start": _mongo_datetime,
    "end": _mongo_datetime,
    "createdAt": _mongo_datetime,
    "updatedAt": _mongo_datetime,
}
ITEM_TO_MONGO = {
    "uid": "_id",
    "summary": "title",
    "description": "description",
    "location": "location",
    "attendee": _item_attendee,
    "dtstart": _item_datetime,
    "dtend": _item_datetime,
    "created": _item_datetime,
    "last-modified": _item_datetime,
}


class Collection(BaseCollection):
    def __init__(self, storage, path):
        self._storage = storage
        self._path = strip_path(path)
        self._user_address = self._path.split("/")[0]
        super().__init__()

    @property
    def path(self):
        """URL path of the collection."""
        # Stored as a property because BaseCollection requires this.
        return self._path

    def get_multi(self, hrefs):
        """Yield (href, item) for each href in given hrefs iterable."""
        hrefs = set(hrefs)  # Drop duplicates
        mongo_ids = list(splitext(href)[0] for href in hrefs)
        for event in self.find({"_id": {"$in": mongo_ids}}):
            # Yield item, remove from original hrefs set
            item = self.mongo_event_to_item(event)
            hrefs.remove(item.href)
            yield (item.href, item)

        for href in hrefs:
            # Yield None for unknown hrefs
            yield (href, None)

    def get_all(self):
        """Yield each item in the collection."""
        for event in self.find():
            yield self.mongo_event_to_item(event)

    def upload(self, href, item):
        """Add or update given item with href path."""
        query = {"_id": splitext(href)[0]}
        if mongo_events := list(self.find(query)):
            # Item found, update event
            (mongo_event,) = mongo_events
            insert = False
            if self.user_id != mongo_event["userId"]:
                raise ValueError("Event modification only allowed for owner")
        else:
            # Item not found, add event
            mongo_event = {
                "userId": self.user_id,
                "guests": [],
                "participants": [],
                "recurrent": False,
                "groups": [],
                "daysOfWeek": None,
                "startTime": None,
                "endTime": None,
            }
            insert = True

        # Loop on vobject children to get event
        for event in item._vobject_item.getChildren():
            if event.name != "VEVENT":
                continue

            # Event found. As iCal allows multiple values for each key, create
            # a dictionary to store a list of fields for each.
            grouped_fields = defaultdict(list)
            for field in event.getChildren():
                grouped_fields[field.name.lower()].append(field)

            # Translate item values into Mongo fields
            for key, translator in ITEM_TO_MONGO.items():
                fields = grouped_fields.get(key)
                if translator is None:
                    continue
                elif callable(translator):
                    translator(key, fields, event, mongo_event)
                else:
                    if fields:
                        (field,) = fields
                        mongo_event[translator] = field.value
                    else:
                        mongo_event[translator] = None
            break

        # Insert or replace mongo event. Replacing keeps the old unknown
        # fields, so that extra fields in Mongo are not removed each time the
        # iCal event is modified.
        if insert:
            self.collection.insert_one(mongo_event)
        else:
            self.collection.replace_one(query, mongo_event)

        return self.mongo_event_to_item(mongo_event)

    def delete(self, href=None):
        """Delete collection or item.

        If href is given, remove the item from the collection.

        Otherwise, remove the whole collection.

        """
        query = {"userId": self.user_id}
        if href is not None:
            query["_id"] = splitext(href)[0]
        self.collection.delete_many(query)

    def get_meta(self, key=None):
        """Get collection metadata.

        If key is given, return the value corresponding to this key, or None if
        the key is not present.

        Otherwise, return the whole properties dictionary.

        """
        # Needs more code if we want to store metadata in the database
        props = {}
        if self.path.endswith("/calendar.ics"):
            props["tag"] = "VCALENDAR"
        return props if key is None else props.get(key)

    def set_meta(self, props):
        """Store the given props as collection metadata."""
        # Needs code if we want to store metadata in the database
        return

    @property
    def last_modified(self):
        if events := tuple(self.find()):
            last_modified = datetime.now(utc)
        else:
            last_modified = max(event["updatedAt"] for event in events)
        return last_modified.strftime("%a, %d %b %Y %H:%M:%S %Z")

    def sync(self, old_token=None):
        """Get latest token and modified hrefs since old token was created."""
        # Update history, get current etag and history
        etag, path_history = self.update_history()

        token = f"http://radicale.org/ns/sync/{etag}"
        if old_token == token:
            # Client etag is current server etag, nothing changed
            return token, set()

        if old_token is None:
            old_etag = None
        else:
            old_etag = old_token[len("http://radicale.org/ns/sync/") :]

        if old_etag in path_history:
            # Old etag is known, only return changes since then
            index = tuple(path_history).index(old_etag) + 1
        else:
            # Old etag is unknown, return everything
            index = 0

        # Build the set of hrefs changed in history since old token
        hrefs = set()
        for values in tuple(path_history.values())[index:]:
            for mongo_id in values["changed"]:
                hrefs.add(f"{mongo_id}.ics")

        return token, hrefs

    # Non-standard API below.

    def get_subcollections(self):
        """Yield collections included in this collection."""
        if self.path == "":
            # Root path, yield user collection
            yield type(self)(self._storage, f"/{self._user_address}")
        elif "/" not in self.path:
            # User path, yield calendar.ics collection
            yield type(self)(self._storage, f"/{self.path}/calendar.ics")

    def mongo_event_to_item(self, mongo_event):
        """Transform Mongo event into a Radicale item."""
        # Create new iCal object
        vobject_item = vobject.newFromBehavior("vcalendar")
        vobject_event = vobject_item.add("vevent")

        # Insert Mongo fields into iCal object
        for key, value in mongo_event.items():
            translator = MONGO_TO_ITEM.get(key)
            if translator is None:
                continue
            elif callable(translator):
                translator(key, value, vobject_event, mongo_event)
            elif value:
                vobject_event.add(translator).value = value

        # Find last-modified value, and set dtstamp if needed
        if hasattr(vobject_event, "last_modified"):
            last_modified = vobject_event.last_modified.value
        else:
            last_modified = datetime.now(utc)

        # The "dtstamp" field is required by the vobject library, or it’s
        # automatically set to a random value. We have to manualyl set one to
        # keep having the same result when the method is called multiple times.
        if not hasattr(vobject_event, "dtstamp"):
            vobject_event.add("dtstamp").value = last_modified

        return Item(
            collection=self,
            href=f'{mongo_event["_id"]}.ics',
            vobject_item=vobject_item,
            last_modified=last_modified.strftime("%a, %d %b %Y %H:%M:%S %Z"),
        )

    @property
    def collection(self):
        """Get Mongo collection."""
        return self._storage.database[
            self._storage.configuration.get("storage", "collection_name")
        ]

    def find(self, query=None):
        """Find events matching given query, with default user-based filter."""
        if not self.user_id:
            return ()

        event_query = {
            "$or": [
                {"userId": self.user_id},
                {"participants._id": self.user_id},
            ]
        }
        event_query.update(query or {})
        return self.collection.find(event_query)

    @property
    def user_id(self):
        """Get user ID from mail address.

        Must be overridden if the user ID in authentication and URL (generally
        the user mail address) is different from the user ID used in Mongo.

        """
        return self._user_address

    def update_history(self):
        """Update history of modifications.

        For each collection, a dictionary of "etag: values" is stored.

        Values for each etag are:
        - date (as a Python datetime),
        - state (as a set of current hrefs),
        - changed (as a set of hrefs that have been modified since last etag).

        Collection history is automatically cleaned, following the
        max_sync_token_age configuration value.

        """
        values = {
            "date": datetime(1, 1, 1),
            "state": set(),
            "changed": set(),
        }

        # Calculate collection etag
        sha = sha256()
        events = tuple(self.find())
        for event in events:
            sha.update(str(event).encode("utf-8"))
        etag = b64encode(sha.digest()).decode("ascii")

        if path_history := HISTORY.get(self.path):
            # History found for this collection
            if etag in path_history:
                # etag is already known, nothing to do
                return etag, path_history
            old_values = tuple(path_history.values())[-1]
        else:
            # No history yet for this collection, create everything
            path_history = HISTORY[self.path] = {}
            old_values = values.copy()

        # Calculate values
        values["date"] = datetime.utcnow()
        for event in events:
            if event["updatedAt"] > old_values["date"]:
                # Event changed since last time
                values["changed"].add(event["_id"])
            values["state"].add(event["_id"])
        # Add deleted events into the list of changed events
        values["changed"] |= old_values["state"] - values["state"]

        # Store values
        path_history[etag] = values

        # Remove old etags
        old_path_history = {  # Keep complete path history before cleaning
            etag: values.copy() for etag, values in path_history.items()
        }
        old_changed, old_values = set(), None
        max_age = self._storage.configuration.get("storage", "max_sync_token_age")
        max_date = datetime.utcnow() - timedelta(seconds=int(max_age))
        for old_etag, old_values in list(path_history.items())[:-1]:
            if old_values["date"] > max_date:
                break
            path_history.pop(old_etag)
            old_changed |= old_values["changed"]
        path_history[list(path_history)[0]]["changed"] |= old_changed

        # Return current etag and complete path history
        return etag, old_path_history


class Storage(BaseStorage):
    Collection = Collection

    def __init__(self, configuration):
        super().__init__(configuration.copy(PLUGIN_CONFIG_SCHEMA))
        self.db_url = self.configuration.get("storage", "db_url")
        self.db_name = self.configuration.get("storage", "db_name")

        # Mongo disconnections may have to be handled correctly
        self.database = MongoClient(self.db_url)[self.db_name]

    def discover(self, path, depth="0"):
        """Yield collections and items stored in this storage.

        If depth is 0, only yield item or collection at given path.

        Otherwise, if the given path leads to a collection, yield its
        direct subcollections and its items.

        """
        sane_path = strip_path(str(path))
        path_parts = sane_path.split("/")

        href = None
        if 2 <= len(path_parts) <= 3:
            if path_parts[1] != "calendar.ics":
                # Unknown path, return
                return
            if len(path_parts) == 3:
                # Item, only keep collection and store item href
                href = path_parts.pop()
        elif len(path_parts) > 3:
            # Unknown path, return
            return

        sane_path = "/".join(path_parts)
        collection = self.Collection(self, unstrip_path(sane_path, trailing_slash=True))

        if href:
            if items := list(collection.get_multi([href])):
                # Yield known item
                yield items[0][1]
            # Unknown href, return
            return

        yield collection

        if depth != "0":
            # Yield children
            yield from collection.get_all()
            yield from collection.get_subcollections()

    def move(self, item, to_collection, to_href):
        """Move the item from one collection to another collection at path."""
        # May be optimized
        to_collection.upload(to_href, item)
        item.collection.delete(item.href)

    def create_collection(self, href, items=None, props=None):
        """Create and return a collection."""
        raise ValueError("Collection creation is forbidden")

    @contextmanager
    def acquire_lock(self, mode, user=None):
        """Context manager locking the storage to avoid concurrent access."""
        # Locks can be achieved with Mongo transactions, requiring extra code
        # (see the "transactions" branch) and server-side configuration
        yield

    def verify(self, folder=None):
        """Verify that the storage is in consistent state."""
        # Needs code if we want to check the database with Radicale
        return True
