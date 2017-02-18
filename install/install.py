import pandas

print('Verifying dependencies...')

assert pandas.__version__ == '0.19.2', 'Invalid pandas version'