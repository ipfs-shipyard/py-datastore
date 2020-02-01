#!/usr/bin/python3
import os.path
import sys
import typing

import mypy.main
import mypy.modulefinder

PEP517_OVERRIDE_MODULES = (
	"pytest",
	"trio",
)


# Patch method of `mypy.modulefinder.FindModuleCache` to ignore that fact that
# some modules do not currently advertise their partial/incomplete typing
# support that we'd like to use anyway
def _find_module_non_stub_helper(
		self: mypy.modulefinder.FindModuleCache,
		components: typing.List[str],
		pkg_dir: str
) -> typing.Optional[typing.Tuple[str, bool]]:
	dir_path = pkg_dir
	for index, component in enumerate(components):
		dir_path = os.path.join(dir_path, component)
		if self.fscache.isfile(os.path.join(dir_path, 'py.typed')) \
		   or component in PEP517_OVERRIDE_MODULES:
			return os.path.join(pkg_dir, *components[:-1]), index == 0
	return None


mypy.modulefinder.FindModuleCache._find_module_non_stub_helper = _find_module_non_stub_helper


sys.exit(mypy.main.main(None, sys.stdout, sys.stderr, [
	"--namespace-packages"
] + sys.argv[1:]))
