def safe_int_conversion(value, default, min_val=None, max_val=None):
    try:
        result = int(value)

        if max_val:
            result = min(result, max_val)
        elif min_val:
            result = max(result, min_val)
    except (TypeError, ValueError):
        result = default

    return result
