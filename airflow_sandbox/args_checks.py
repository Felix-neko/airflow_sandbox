def wheres_my_args_dude(*args, **kwargs):
    what_to_print = f"""

=============================
ARGS (len == {len(args)})
=============================
{args}

=============================
KWARGS (len == {len(kwargs)})
=============================
{list(kwargs.keys())}
{kwargs}


"""
    print(what_to_print)