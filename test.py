# Python 3 code to demonstrate the 
# working of MD5 (string - hexadecimal)
  
import hashlib
  
# initializing string
str2hash = "ddo.2017"
  
# encoding GeeksforGeeks using encode()
# then sending to md5()
print(str2hash.encode())
result = hashlib.md5(b''str2hash)
  
# printing the equivalent hexadecimal value.
print("The hexadecimal equivalent of hash is : ", end ="")
print(result.hexdigest())

# 52400ede39b6a2098dc0ffb5aad536e6
# 52400ede39b6a2098dc0ffb5aad536e6