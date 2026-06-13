import optparse
import os
import subprocess
import shutil
import sys

def main():
    parser = optparse.OptionParser()
    parser.add_option('--output_root_directory')
    parser.add_option('--built_packages_root_directory')
    parser.add_option('--test', metavar='dotted name')
    parser.add_option('--skip-setup', action='store_true', dest='skip_setup', default=False)
    options, _ = parser.parse_args()

    # The plugin is already installed by the testing environment's custom setup for l3kvg.
    
    test = options.test or 'test_ils'

    rc = 0
    try:
        # Bootstrap the L3KVG catalog
        print("Bootstrapping L3KVG Catalog...")
        subprocess.check_call(['sudo', '/usr/bin/bootstrap_l3kvg'])
        
        test_output_file = '/var/lib/irods/log/test_output.log'
        cmd = f'set -o pipefail; python3 scripts/run_tests.py --xml_output --run_s {test} 2>&1 | tee {test_output_file}'
        subprocess.check_call(['sudo', 'su', '-', 'irods', '-c', cmd])
    except subprocess.CalledProcessError as e:
        rc = e.returncode
    except Exception as e:
        print(f"Exception during test: {e}")
        rc = 1
    finally:
        output_root_directory = options.output_root_directory
        if output_root_directory:
            if not os.path.exists(output_root_directory):
                os.makedirs(output_root_directory)
            
            # Copy logs out
            log_dir = '/var/lib/irods/log'
            if os.path.exists(log_dir):
                for f in os.listdir(log_dir):
                    full_path = os.path.join(log_dir, f)
                    if os.path.isfile(full_path):
                        try:
                            shutil.copy(full_path, output_root_directory)
                        except: pass
            
            # Copy test reports
            report_dir = '/var/lib/irods/test-reports'
            if os.path.exists(report_dir):
                try:
                    shutil.copytree(report_dir, os.path.join(output_root_directory, 'test-reports'), dirs_exist_ok=True)
                except: pass

    sys.exit(rc)

if __name__ == '__main__':
    main()
