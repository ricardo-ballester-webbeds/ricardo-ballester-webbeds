import hashlib

# Request input from the user
user_input = input("Enter a string: ")

# Calculate the SHA-1 hash of the input
sha1_hash = hashlib.sha1(user_input.encode()).hexdigest()

# Print the SHA-1 hash
print(f'SHA-1 Hash: {sha1_hash}')