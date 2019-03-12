# -*- coding: utf-8 -*-

__author__ = 'Ryan Anguiano'
__email__ = 'ryan.anguiano@gmail.com'
__version__ = '0.0.1'


import calendar
import functools
import operator
from datetime import datetime

try:
    import pytz
except ImportError:  # pragma: no cover
    pytz = None


__all__ = ['AsyncTimeSeries', 'seconds', 'minutes', 'hours', 'days']


seconds = lambda i: i
minutes = lambda i: i * seconds(60)
hours = lambda i: i * minutes(60)
days = lambda i: i * hours(24)


class AsyncTimeSeries:
    granularities = {
        '1minute': {'duration': minutes(1), 'ttl': hours(1)},
        '5minute': {'duration': minutes(5), 'ttl': hours(6)},
        '10minute': {'duration': minutes(10), 'ttl': hours(12)},
        '1hour': {'duration': hours(1), 'ttl': days(7)},
        '1day': {'duration': days(1), 'ttl': days(31)},
    }

    def __init__(self, client, base_key='stats', use_float=False,
                 timezone=None, granularities=None):
        self.client = client
        self.base_key = base_key
        self.use_float = use_float
        self.timezone = timezone
        self.granularities = granularities or self.granularities
        self.chain = self.client.pipeline()

    def get_key(self, key, timestamp, granularity):
        ttl = self.granularities[granularity]['ttl']
        timestamp_key = round_time(timestamp, ttl)  # No timezone offset in the key
        return ':'.join([self.base_key, granularity, str(timestamp_key), str(key)])

    async def increase(self, key, amount, timestamp=None, execute=True):
        pipe = self.client.pipeline() if execute else self.chain

        for granularity, props in self.granularities.items():
            hkey = self.get_key(key, timestamp, granularity)
            bucket = round_time_with_tz(timestamp, props['duration'], self.timezone)

            _incr = pipe.hincrbyfloat if self.use_float else pipe.hincrby
            _incr(hkey, bucket, amount)

            pipe.expire(hkey, props['ttl'])

        if execute:
            await pipe.execute()

    async def decrease(self, key, amount, timestamp=None, execute=True):
        await self.increase(key, -1 * amount, timestamp, execute)

    async def execute(self):
        results = await self.chain.execute()
        self.chain = self.client.pipeline()
        return results

    async def get_buckets(self, key, granularity, count, timestamp=None):
        props = self.granularities[granularity]
        if count > (props['ttl'] / props['duration']):
            raise ValueError('Count exceeds granularity limit')

        pipe = self.client.pipeline()
        buckets = []
        rounded = round_time_with_tz(timestamp, props['duration'], self.timezone)
        bucket = rounded - (count * props['duration'])

        for _ in range(count):
            bucket += props['duration']
            buckets.append(unix_to_dt(bucket))
            pipe.hget(self.get_key(key, bucket, granularity), bucket)

        _type = float if self.use_float else int
        parse = lambda x: _type(x or 0)

        results = map(parse, await pipe.execute())

        return list(zip(buckets, results))

    async def get_total(self, *args, **kwargs):
        return sum([
            amount for bucket, amount in await self.get_buckets(*args, **kwargs)
        ])

    async def scan_keys(self, granularity, count, search='*', timestamp=None):
        props = self.granularities[granularity]
        if count > (props['ttl'] / props['duration']):
            raise ValueError('Count exceeds granularity limit')

        hkeys = set()
        prefixes = set()
        rounded = round_time_with_tz(timestamp, props['duration'], self.timezone)
        bucket = rounded - (count * props['duration'])

        for _ in range(count):
            bucket += props['duration']
            hkeys.add(self.get_key(search, bucket, granularity))
            prefixes.add(self.get_key('', bucket, granularity))

        pipe = self.client.pipeline()
        for key in hkeys:
            pipe.keys(key)
        results = functools.reduce(operator.add, await pipe.execute())

        parsed = set()
        for result in results:
            result = result.decode('utf-8')
            for prefix in prefixes:
                result = result.replace(prefix, '')
            parsed.add(result)

        return sorted(parsed)

    async def record_hit(self, key, timestamp=None, count=1, execute=True):
        await self.increase(key, count, timestamp, execute)

    async def remove_hit(self, key, timestamp=None, count=1, execute=True):
        await self.decrease(key, count, timestamp, execute)

    get_hits = get_buckets
    get_total_hits = get_total


def round_time(dt, precision):
    seconds = dt_to_unix(dt or tz_now())
    return int((seconds // precision) * precision)


def round_time_with_tz(dt, precision, tz=None):
    rounded = round_time(dt, precision)

    if tz and precision % days(1) == 0:
        rounded_dt = unix_to_dt(rounded).replace(tzinfo=None)
        offset = tz.utcoffset(rounded_dt).total_seconds()
        rounded = int(rounded - offset)

        dt = unix_to_dt(dt or tz_now())
        dt_seconds = (hours(dt.hour) + minutes(dt.minute) + seconds(dt.second))
        if offset < 0 and dt_seconds < abs(offset):
            rounded -= precision
        elif offset > 0 and dt_seconds >= days(1) - offset:
            rounded += precision

    return rounded


def tz_now():
    if pytz:
        return datetime.utcnow().replace(tzinfo=pytz.utc)
    else:
        return datetime.now()


def dt_to_unix(dt):
    if isinstance(dt, datetime):
        dt = calendar.timegm(dt.utctimetuple())
    return dt


def unix_to_dt(dt):
    if isinstance(dt, (int, float)):
        utc = pytz.utc if pytz else None
        dt = datetime.fromtimestamp(dt, utc)
    return dt
