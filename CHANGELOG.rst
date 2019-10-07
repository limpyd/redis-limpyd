Changelog
=========

Release *v2.0.dev7* - ``2019-10-07``
------------------------------------
* Support for redis-py >= 3 only
* Support for redis-server >= 3 only
* Breaking change: `zadd` value/scores cannot be passed as positional arguments anymore
* Breaking change: `zadd` flags other than `ch` are explicitely not supported
* Breaking change: `zincrby` arguments are swaped (`amount, value` instead of `value, amount`)
* Breaking change: Redis server with `LUA` scripting support is mandatory
* Breaking change: `set` flags other than `ex` and `px` are explicitely not supported
* Add `decrby`, `incrby` and `bitpos` to `StringField`
* Add expiring commands to all normal fields (not `InstanceHashField` and `*PKField`): `expire`, `pexpire`, `expireat`, `pexpireat`, `ttl`, `pttl`, `persite`. But `*expire*` commands can only be called on non-indexable fields
* Add `setex` and `psetex` to `StringField`. Can only be called on non-indexable fields.
* Deny `ex` and `px` flag to `set` if field is indexable
* Add support for `count` argument to `spop` (only for redis-server >= 3.2)
* Add new commands to `ListField`: `lcontains`, `lrank` and `lcount`, to know if a value is in the list, where, and how many times. This is done on the redis server side via lua scripting.
* Optimize deindexing when calling `hmset` or `hdel`

Release *v1.3.1* - ``2019-10-11``
---------------------------------
* Resolve race condition in `get_or_connect`

Release *v1.3* - ``2019-09-22``
-------------------------------
* Official support for Python 3.7 and 3.8
* Remove support for Python 3.4

Release *v1.2* - ``2018-01-31``
-------------------------------
* Repair packaging

Release *v1.1* - ``2018-01-30``
-------------------------------
* BROKEN RELEASE, sorry
* Official support for redis-py 2.10.6
* Resolve two race conditions (``get`` and more important, ``pipeline``)
* Add *scan* methods for databases/models/instances/fields (sets, hsets, zsets)
* Add *sort* methods for sets, lists, zsets

Release *v1.0.1* - ``2018-01-30``
---------------------------------
* BROKEN RELEASE, sorry
* Official support for PyPy & PyPy3

Release *v1.0* - ``2018-01-29``
-------------------------------
* BROKEN RELEASE, sorry
* Add real indexing capabilities
* Correct/enhance slicing
* Remove support for python 3.3 (keeps 2.7 and 3.4 to 3.6)

Release *v0.2.4* - ``2015-12-16``
---------------------------------

* Locally solve a locking bug in redis-py

Release *v0.2.3* - ``2015-12-16``
---------------------------------

* Compatibility with Redis-py 2.10

Release *v0.2.2* - ``2015-06-12``
---------------------------------

* Compatibility with pip 6+

Release *v0.2.1* - ``2015-01-12``
---------------------------------

* Stop using dev version of "future"

Release *v0.2.0* - ``2014-09-07``
---------------------------------

* Adding support for python 3.3 and 3.4

Release *v0.1.3* - ``2013-09-07``
---------------------------------

* Add the missing 'hdel' command to the RedisModel class

Release *v0.1.2* - ``2013-08-30``
---------------------------------

* Add the missing 'delete' command to the HashField field

Release *v0.1.1* - ``2013-08-26``
---------------------------------

* Include only the "limpyd" package in setup.py (skip the tests)

Release *v0.1.0* - ``2013-02-12``
---------------------------------

* First public version
