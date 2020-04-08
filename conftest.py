from hypothesis import settings

# define tests profile. Full documentation is at:
# https://hypothesis.readthedocs.io/en/latest/settings.html#settings-profiles
settings.register_profile("fast", max_examples=5, deadline=5000)
settings.register_profile("slow", max_examples=20, deadline=5000)

# Ignore the following modules because wsgi module fails as no
# configuration file is found (--doctest-modules forces the module
# loading)
collect_ignore = ["swh/indexer/storage/api/wsgi.py"]
