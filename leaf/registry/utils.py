def inheritance_depth(cls: type, base_cls: type) -> int:
    """
    Calculates the inheritance depth of `cls` from the given `base_cls`.

    Returns:
        The number of steps between `cls` and `base_cls` in the inheritance chain.

    Raises:
        ValueError: If `base_cls` is not in the inheritance hierarchy of `cls`.
    """
    depth = 0
    seen = set()
    while cls and cls is not base_cls:
        if cls in seen:
            raise ValueError(f"Circular inheritance detected involving {cls.__name__}")
        seen.add(cls)
        cls = cls.__base__
        depth += 1

    if cls is not base_cls:
        raise ValueError(f"{base_cls.__name__} is not a base class of {cls.__name__}")

    return depth


# Constant used for locating adapter codes in metadata
ADAPTER_ID_KEY = "adapter_id"
