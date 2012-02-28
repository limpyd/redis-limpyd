def make_key(*args):
    """Create the key concatening all args with `:`."""
    return u":".join(unicode(arg) for arg in args)
