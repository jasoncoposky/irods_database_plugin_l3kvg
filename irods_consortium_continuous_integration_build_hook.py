import multiprocessing
import optparse
import os
import sys
import tempfile

import irods_python_ci_utilities

def update_local_package_repositories():
    dispatch_map = {
        'Ubuntu': ['sudo', 'apt-get', 'update'],
        'Debian gnu_linux': ['sudo', 'apt-get', 'update']
    }
    try:
        cmd = dispatch_map[irods_python_ci_utilities.get_distribution()]
        if cmd:
            irods_python_ci_utilities.subprocess_get_output(cmd, check_rc=True)
    except KeyError:
        pass # Ignore or handle other distros if needed

def install_building_dependencies(externals_directory):
    # Standard iRODS externals required for building
    externals_list = [
        'irods-externals-cmake3.21.4-0',
        'irods-externals-clang16.0.6-0',
        'irods-externals-fmt9.1.0-1'
    ]
    if externals_directory == 'None' or externals_directory is None:
        irods_python_ci_utilities.install_irods_core_dev_repository()
        irods_python_ci_utilities.install_os_packages(externals_list)
    
    # Install L3KVG specific dependencies
    update_local_package_repositories()
    irods_python_ci_utilities.install_os_packages([
        'libzmq3-dev',
        'libfmt-dev',
        'libxml2-dev',
        'libssl-dev',
        'gcc',
        'g++',
        'make'
    ])

def copy_output_packages(build_directory, output_root_directory):
    irods_python_ci_utilities.gather_files_satisfying_predicate(
        build_directory,
        irods_python_ci_utilities.append_os_specific_directory(output_root_directory),
        lambda s:s.endswith(irods_python_ci_utilities.get_package_suffix()))

def main(build_directory, output_root_directory, irods_packages_root_directory, externals_directory, irods_package_version):
    install_building_dependencies(externals_directory)
    
    if irods_package_version is not None:
        irods_python_ci_utilities.install_irods_packages_repository()
        irods_python_ci_utilities.install_released_irods_dev_and_runtime_packages(irods_package_version)
    elif irods_packages_root_directory:
        irods_python_ci_utilities.install_irods_dev_and_runtime_packages(irods_packages_root_directory)
        
    build_directory = os.path.abspath(build_directory or tempfile.mkdtemp(prefix='irods_l3kvg_plugin_build_directory'))
    
    # Run CMake and Make
    irods_python_ci_utilities.subprocess_get_output(['cmake', os.path.dirname(os.path.realpath(__file__))], check_rc=True, cwd=build_directory)
    irods_python_ci_utilities.subprocess_get_output(['make', '-j', str(multiprocessing.cpu_count()), 'package'], check_rc=True, cwd=build_directory)
    
    if output_root_directory:
        copy_output_packages(build_directory, output_root_directory)

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('--build_directory')
    parser.add_option('--output_root_directory')
    parser.add_option('--irods_packages_root_directory')
    parser.add_option('--externals_packages_directory')
    parser.add_option('--irods_package_version')
    options, _ = parser.parse_args()

    main(options.build_directory,
         options.output_root_directory,
         options.irods_packages_root_directory,
         options.externals_packages_directory,
         options.irods_package_version)
