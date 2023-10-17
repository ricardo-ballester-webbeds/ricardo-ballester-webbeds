import hashlib

def generate_sha1(input_string):
    sha1 = hashlib.sha1()
    sha1.update(input_string.encode('utf-8'))
    return sha1.hexdigest()

input_string = input("Write here your text")
sha1_hash = generate_sha1(input_string)
print("SHA1 hash of input string:", sha1_hash)
