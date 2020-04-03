import importlib
from typing import Callable, List, Tuple, Type, TypeVar, Union

import datastore

T_co = TypeVar("T_co", covariant=True)


def make_datastore_test_params(adapter: str, subname: str = "") \
    -> Tuple[str, Tuple[
        Tuple[
            Type[datastore.abc.BinaryAdapter],
            Type[Union[datastore.BinaryDictDatastore, datastore.ObjectDictDatastore]],
            Callable[[T_co], bytes]
        ],
        Tuple[
            Type[datastore.abc.ObjectAdapter],
            Type[datastore.ObjectDictDatastore],
            Callable[[T_co], List[T_co]]
        ]
    ]]:
	mod = importlib.import_module(f"datastore.adapter.{adapter}")
	
	def encode_bin(value: T_co) -> bytes:
		return str(value).encode()
	
	def encode_obj(value: T_co) -> List[T_co]:
		return [value]
	
	return ("Adapter, DictDatastore, encode_fn", (
		(getattr(mod, f"Binary{subname}Adapter"), datastore.BinaryDictDatastore, encode_bin),  # type: ignore
		(getattr(mod, f"Object{subname}Adapter"), datastore.ObjectDictDatastore, encode_obj)   # type: ignore
	))
