def make_foo(*args, **kwargs):
    print("---> making foo!")
    print("make foo(...): args")
    print(args)
    print("make foo(...): kwargs")
    print(kwargs)