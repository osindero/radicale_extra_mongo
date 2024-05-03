import radicale_storage_mongo
from dateutil.rrule import rrulestr, rrule
from dateutil.parser import parse
from bson.objectid import ObjectId
from pymongo import MongoClient
from icalendar import vRecur
from dateutil.rrule import SECONDLY, MINUTELY, HOURLY, DAILY, WEEKLY, MONTHLY, YEARLY
from vobject.icalendar import dateTimeToString, utc
from dateutil.rrule import rrulestr, HOURLY, MINUTELY, SECONDLY, DAILY, WEEKLY, MONTHLY, YEARLY
from datetime import datetime, timezone
import vobject
import io
import datetime
import isodate

FREQ_MAP = {
    0: 'YEARLY',
    1: 'MONTHLY',
    2: 'WEEKLY',
    3: 'DAILY',
    4: 'HOURLY',
    5: 'MINUTELY',
    6: 'SECONDLY'
}

# MongoDB client setup
client = MongoClient("mongodb://10.211.55.2:27017")  # Connect to your MongoDB
db = client.base  # Select your database
collection = db.radicale  # Select your collection

def rrule_to_dict(rrule):
    """Converts a dateutil rrule object to a dictionary."""
    print(f'Converting rrule to dictionary: {rrule}')
    result = {
        'freq': rrule._freq,
        'dtstart': rrule._dtstart.isoformat(),
        'interval': rrule._interval,
        'wkst': rrule._wkst,
        'count': rrule._count,
        'until': rrule._until.isoformat() if rrule._until else None,
        'bysetpos': list(rrule._bysetpos) if isinstance(rrule._bysetpos, set) else rrule._bysetpos,
        'bymonth': list(rrule._bymonth) if isinstance(rrule._bymonth, set) else rrule._bymonth,
        'bymonthday': list(rrule._bymonthday) if isinstance(rrule._bymonthday, set) else rrule._bymonthday,
        'byyearday': list(rrule._byyearday) if isinstance(rrule._byyearday, set) else rrule._byyearday,
        'byweekno': list(rrule._byweekno) if isinstance(rrule._byweekno, set) else rrule._byweekno,
        'byweekday': [wd.weekday for wd in rrule._byweekday] if hasattr(rrule._byweekday, '__iter__') and not isinstance(rrule._byweekday[0], int) else None,
        'byhour': list(rrule._byhour) if isinstance(rrule._byhour, set) else rrule._byhour,
        'byminute': list(rrule._byminute) if isinstance(rrule._byminute, set) else rrule._byminute,
        'bysecond': list(rrule._bysecond) if isinstance(rrule._bysecond, set) else rrule._bysecond,
    }
    print(f'Debug: Converted rrule to dictionary: {result}')
    return result


def dict_to_rrule(dict_rrule):
    """
    Converts a dictionary to an iCalendar RRULE string
    """
    freq = dict_rrule.get('freq', None)
    if freq not in FREQ_MAP:
        raise ValueError(f"Unsupported frequency: {freq}")

    freq_str = FREQ_MAP[freq]
    
    rrule_str = f'FREQ={freq_str.upper()};'
    
    if 'interval' in dict_rrule:
        rrule_str += f'INTERVAL={dict_rrule.get("interval", 1)};'
    if 'bysecond' in dict_rrule:
        rrule_str += f'BYSECOND={",".join(map(str, dict_rrule.get("bysecond")))};'
    if 'byminute' in dict_rrule:
        rrule_str += f'BYMINUTE={",".join(map(str, dict_rrule.get("byminute")))};'
    if 'byhour' in dict_rrule:
        rrule_str += f'BYHOUR={",".join(map(str, dict_rrule.get("byhour")))};'
    if 'until' in dict_rrule and dict_rrule.get("until"):
        until_date = parse(dict_rrule.get("until"))
        if until_date.tzinfo:
            # Convert to UTC representation
            until_str = until_date.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        else:
            # Use as local time representation
            until_str = until_date.strftime('%Y%m%dT%H%M%S')
        rrule_str += f'UNTIL={until_str};'


    # Add more fields as necessary

    final_rrule = rrule_str.rstrip(';')
    print(f"Generated RRULE: {final_rrule}")
    return final_rrule


def item_rrule_to_mongo(key, fields, vobject_event, mongo_event):
    """Read item "RRULE" fields and store them in Mongo event."""
    print(f'Reading item RRULE fields from vobject_event: key={key}')
    if 'rrule' in vobject_event.contents:
        rrule_field = vobject_event.contents['rrule'][0]
        print(f'RRULE details: {rrule_field.value}')  # Print the details of the RRULE property
        rrule_obj = rrulestr(rrule_field.value)
        mongo_event['my_rrule'] = rrule_to_dict(rrule_obj)
    else:
        print('No RRULE field in this vobject event.')

def mongo_rrule_to_item(key, value, vobject_event, mongo_event):
    """Adds a recurrence rule (rrule) from a MongoDB event to a vObject event."""
    print(f'Reading Mongo rrule field and storing it in Radicale item: key={key}, value={value}')
    vobject_rrule = vobject_event.add('rrule')
    vobject_rrule.value = dict_to_rrule(value)  # this should be a string now


def item_categories_to_mongo(key, fields, vobject_event, mongo_event):
    """Read item "CATEGORIES" fields and store them in Mongo event."""
    print(f'Reading item CATEGORIES fields from vobject_event: key={key}')
    if 'categories' in vobject_event.contents:
        categories_field = vobject_event.contents['categories'][0]
        # Check if categories_field.value is a string and contains a comma
        if isinstance(categories_field.value, str) and ',' in categories_field.value:
            mongo_event['categories'] = categories_field.value.split(',')
        else:
            # It's either a single category (a string without commas) or already a list
            mongo_event['categories'] = categories_field.value

def mongo_categories_to_item(key, value, vobject_event, mongo_event):
    """Adds categories from a MongoDB event to a vObject event."""
    print(f'Reading Mongo categories field and storing it in Radicale item: key={key}, value={value}')
    vobject_categories = vobject_event.add('categories')
    # Since value is always a list, no need to check its type
    vobject_categories.value = value
    print(f'Assigned value to vobject_categories: {vobject_categories.value}')

def mongo_alarm_to_item(key, value, vobject_event, mongo_event):
    print(f'Reading Mongo alarm field and storing it in Radicale item: key={key}, value={value}')
    for alarm_dict in value:
        vobject_alarm = vobject_event.add('valarm')
        for field_key, field_value in alarm_dict.items():
            vobject_field = vobject_alarm.add(field_key)
            if isinstance(field_value, str):
                if field_key.lower() == 'trigger':
                    # Handle ISO 8601 duration strings for trigger
                    try:
                        vobject_field.value = isodate.parse_duration(field_value)
                    except isodate.ISO8601Error:
                        # Not a duration string, handle as usual
                        vobject_field.value = field_value
                else:
                    # Add a root component and parse the string to a vObject
                    field_value = f"BEGIN:VCALENDAR\n{field_value}\nEND:VCALENDAR"
                    stream = io.StringIO(field_value)
                    vobject_component = vobject.readComponents(stream).__next__()

                    # Extract the value from the parsed component
                    for comp in vobject_component.getChildren():
                        if comp.name.lower() == field_key.lower():
                            vobject_field.value = comp.value
                            break
            else:
                vobject_field.value = field_value

def _item_alarm(key, fields, vobject_event, mongo_event):
    if fields is None:
        fields = []

    for field in fields:
        if isinstance(field, list):
            for sub_field in field:
                alarm_dict = {}
                for param_key, param_value in sub_field.params.items():
                    alarm_dict[param_key.lower()] = param_value
                for component in sub_field.getChildren():
                    if component.name.lower() == 'trigger' and isinstance(component.value, datetime.timedelta):
                        # Store timedelta objects as ISO 8601 duration string
                        alarm_dict[component.name.lower()] = isodate.duration_isoformat(component.value)
                    elif hasattr(component, 'value'):
                        alarm_dict[component.name.lower()] = component.value
                mongo_event[key].append(alarm_dict)
        else:
            if field.name.lower() == 'trigger' and isinstance(field.value, datetime.timedelta):
                # Store timedelta objects as ISO 8601 duration string
                mongo_event[key] = isodate.duration_isoformat(field.value)
            elif hasattr(field, 'value'):
                mongo_event[key] = field.value

# Map the 'RRULE' iCal field to the 'my_rrule' Mongo field
radicale_storage_mongo.ITEM_TO_MONGO['RRULE'] = item_rrule_to_mongo
radicale_storage_mongo.MONGO_TO_ITEM['my_rrule'] = mongo_rrule_to_item
radicale_storage_mongo.ITEM_TO_MONGO['CATEGORIES'] = item_categories_to_mongo
radicale_storage_mongo.MONGO_TO_ITEM['categories'] = mongo_categories_to_item
radicale_storage_mongo.MONGO_TO_ITEM['alarm'] = mongo_alarm_to_item
radicale_storage_mongo.ITEM_TO_MONGO['valarm'] = _item_alarm

class Storage(radicale_storage_mongo.Storage):
    pass

PLUGIN_CONFIG_SCHEMA = radicale_storage_mongo.PLUGIN_CONFIG_SCHEMA
