import hashlib
import random
import string


def hash_password(password):
    password_bytes = password.encode('utf-8')
    sha256_hash = hashlib.sha256()
    sha256_hash.update(password_bytes)
    hashed_password = sha256_hash.hexdigest()

    return hashed_password

def generate_token():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=30))