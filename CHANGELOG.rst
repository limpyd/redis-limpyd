Changelog
=========

Release *v1.3.dev0* - ``2019-09-22``
------------------------------------
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
