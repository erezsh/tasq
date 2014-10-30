import importlib
import json

def pack_args(args, kwargs):
    return json.dumps({'args':args, 'kwargs': kwargs})

def unpack_args(args):
    d = json.loads(args)
    return d['args'], d['kwargs']

def find_function(func_module, func_name):
    module = importlib.import_module(func_module)
    return getattr(module, func_name)
