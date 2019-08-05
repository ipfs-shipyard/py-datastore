import hashlib


def fast_hash(to_hash):
    """fast, deterministic hash function"""
    return int(hashlib.sha1(str(to_hash).encode('utf-8')).hexdigest(), 16)
