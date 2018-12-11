#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 consistent hashing for nosql client
 based on ezmobius client (redis-rb)
 and see this article http://amix.dk/blog/viewEntry/19367
"""

import zlib
import bisect
import operator
from hashlib import md5, sha1

from ._compat import xrange, b, long

def murmur(s):
    import murmur2
    
    def to_java_long(pyLong):
        #max long value in java  
        if(pyLong > 9223372036854775807):
            pyLong = (pyLong+2**63)%2**63 - 2**63
        return pyLong
    return to_java_long(murmur2.murmur64a(s, len(s), 0x1234ABCD))


hash_methods = {
    'crc32': lambda x: zlib.crc32(x) & 0xffffffff,
    'md5': lambda x: long(md5(x).hexdigest(), 16),
    'sha1': lambda x: long(sha1(x).hexdigest(), 16),
    'murmur': murmur
}


class HashRing(object):

    """Consistent hash for nosql API"""

    def __init__(self, nodes=[], replicas=128, hash_method='crc32'):
        """Manages a hash ring.

        `nodes` is a list of objects that have a proper __str__ representation.
        `replicas` indicates how many virtual points should be used pr. node,
        replicas are required to improve the distribution.
        `hash_method` is the key generator method.
        """
        self.hash_method = hash_methods[hash_method]
        self.nodes = []
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []

        for n in nodes:
            self.add_node(n)

    def add_node(self, node):
        """Adds a `node` to the hash ring (including a number of replicas).
        """
        self.nodes.append(node)
        for x in xrange(self.replicas):
            ring_key = self.hash_method(b("%s:%d" % (node, x)))
            self.ring[ring_key] = node
            self.sorted_keys.append(ring_key)

        self.sorted_keys.sort()

    def remove_node(self, node):
        """Removes `node` from the hash ring and its replicas.
        """
        self.nodes.remove(node)
        for x in xrange(self.replicas):
            ring_key = self.hash_method(b("%s:%d" % (node, x)))
            self.ring.pop(ring_key)
            self.sorted_keys.remove(ring_key)

    def get_node(self, key):
        """Given a string key a corresponding node in the hash ring is returned.

        If the hash ring is empty, `None` is returned.
        """
        n, i = self.get_node_pos(key)
        return n

    def get_node_pos(self, key):
        """Given a string key a corresponding node in the hash ring is returned
        along with it's position in the ring.

        If the hash ring is empty, (`None`, `None`) is returned.
        """
        if len(self.ring) == 0:
            return [None, None]
        crc = self.hash_method(b(key))
        idx = bisect.bisect(self.sorted_keys, crc)
        # prevents out of range index
        idx = min(idx, (self.replicas * len(self.nodes)) - 1)
        return [self.ring[self.sorted_keys[idx]], idx]

    def iter_nodes(self, key):
        """Given a string key it returns the nodes as a generator that can hold the key.
        """
        if len(self.ring) == 0:
            yield None, None
        node, pos = self.get_node_pos(key)
        for k in self.sorted_keys[pos:]:
            yield k, self.ring[k]

    def __call__(self, key):
        return elf.get_node(key)


class MurmurHashRing(object):

    def __init__(self, nodes=[]):
        self._m = []
        for i, s in enumerate(nodes):
            for n in range(160):
                self._m.append((murmur("SHARD-" + str(i) + "-NODE-" + str(n)), s))
        self._m.sort(key=operator.itemgetter(0))

    def get_node(self, key):
        v = murmur(key)
        for k, s in self._m:
            _s = None
            if v <= k:
                _s = s; break;
            if not _s:
                _s = self._m[0][1]
        return _s
        # i = bisect.bisect_left(self._m, murmur(key))
        # if i >= len(self._m):
        #     return self._m[0][1]
        # else:
        #     return self._m[i][1]
