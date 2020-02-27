import hashlib
import re

def get_index(target_language):
    index_data = {
        'hi': 'en-hi',
        'bn': 'en-bn',
        'gu': 'en-gu',
        'mr': 'en-mr',
        'kn': 'en-kn',
        'te': 'en-te',
        'ml': 'en-ml',
        'pa': 'en-pa',
        'ta': 'en-ta'
    }
    return index_data[target_language]


def get_hash(text):
    encoded_str = hashlib.sha256(text.encode())
    hash_hex = encoded_str.hexdigest()
    return hash_hex


def contains_english_characters(text):
    if not re.search('[a-zA-Z]', text):
        return False
    return True


def get_value(data, key):
    try:
        return data[key]
    except Exception as e:
        return None


def get_lang(target_language):
    index_data = {
        'hi': 'Hindi',
        'bn': 'Bengali',
        'gu': 'Gujarati',
        'mr': 'Marathi',
        'kn': 'Kannada',
        'te': 'Telugu',
        'ml': 'Malayalam',
        'pa': 'Punjabi',
        'ta': 'Tamil',
        'en': 'English'
    }
    return index_data[target_language]

