import os


def safe_get(element, *keys):
    """
    Return dictionary value if *keys (nested) exists in `element` (dict).
    """
    if not isinstance(element, dict):
        raise AttributeError('keys_exists() expects dict as first argument.')
    if len(keys) == 0:
        raise AttributeError('keys_exists() expects at least two arguments, one given.')

    _element = element
    for key in keys:
        _element = _element.get(key)
        if _element is None:
            return None
    return _element


def check_key_exists(element, *keys):
    """
    Check if *keys (nested) exists in `element` (dict).
    """
    if not isinstance(element, dict):
        raise AttributeError('keys_exists() expects dict as first argument.')
    if len(keys) == 0:
        raise AttributeError('keys_exists() expects at least two arguments, one given.')

    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True


def record_bad_response(z_code, *args, filename, extension="csv"):
    bad_record_file = os.path.join(".", "output", filename + "." + extension)
    if not os.path.exists(bad_record_file):
        with open(bad_record_file, "a+") as f:
            if len(args) > 0:
                text_headers = ",".join(["zip_code"] + [args[0]])
            else:
                text_headers = "zip_code"
            f.write("{}\n".format(text_headers))
    with open(bad_record_file, "a+") as f:
        if len(args) > 0:
            text_entries = ",".join([z_code] + [args[1]])
        else:
            text_entries = z_code
        f.write("{}\n".format(text_entries))
