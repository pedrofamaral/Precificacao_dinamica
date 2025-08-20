from jsonschema import Draft7Validator
from .schema_listing_v1 import schema_listing_v1

_validator = Draft7Validator(schema_listing_v1)

def validate_or_warn(doc):
    errs = sorted(_validator.iter_errors(doc), key=lambda e: e.path)
    if not errs: return True, None
    return False, "; ".join([e.message for e in errs])
