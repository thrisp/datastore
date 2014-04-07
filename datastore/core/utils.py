import hashlib


def makehash(tohash):
    return int(hashlib.sha1(str(tohash).encode('utf-8')).hexdigest(), 16)
