import toml

# load configuration variables from .toml file
with open('modules/config.toml', 'r') as f:
    config = toml.load(f)

