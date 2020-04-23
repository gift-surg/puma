#!/usr/bin/env bash
set -eo pipefail

function is_gpu_job {
  # Determines whether or not the current job is a "GPU" job based on it's job name (CI_JOB_NAME).
  # Returns true if job name contains "gpu", false otherwise
  if [[ "${CI_JOB_NAME}" == *"gpu"* ]]; then
    return 0
  else
    return 1
  fi
}

function determine_virtualenv_type {
  # Determines the VirtualEnv type based on the job's name. "gpu" if the job name contains "gpu", "dev" otherwise
  if is_gpu_job; then
    echo "gpu"
  else
    echo "dev"
  fi
}

function determine_pip_extras {
  # Determines the "extras" to "pip install" based on the job's name.
  # "dev,gpu" if the job name contains "gpu", "dev" otherwise
  if is_gpu_job; then
    echo "dev,gpu"
  else
    echo "dev"
  fi
}

function determine_activate_script_path {
  # Returns the path to "activate" script for a given VirtualEnv path
  virtualenv_path="$1"
  echo "${virtualenv_path}/Scripts/activate"
}

function determine_base_virtualenv_name {
  # Detemines a VirtualEnv's name based on the dependencies defined in setup.py

  # List the files that influence the contents of the base virtualenv
  files_to_consider=(setup.cfg setup.py ci-scripts/virtualenv-helpers.sh)
  # Generate an MD5 hash of each of these files - any changes in file content will cause the hash to change
  file_hashes=$(md5sum "${files_to_consider[@]}")
  # Combine the hashes of all files into a single "summary" hash
  virtualenv_id=$(echo "${file_hashes}" | md5sum | cut -d' ' -f1)

  # Print out the base virtualenvs full id
  echo "puma-ci-$(determine_virtualenv_type)-${virtualenv_id}"
}

function ensure_base_virtualenv_exists {
  # Ensures a base VirtualEnv exists
  # This VirtualEnv is defined by the dependencies defined in setup.py

  virtualenv_name=$(determine_base_virtualenv_name)
  virtualenv_path="${WORKON_HOME}/${virtualenv_name}"
   if [ -d "${virtualenv_path}" ]; then
    echo "Base VirtualEnv (${virtualenv_name}) already exists, ending early"
    return
  fi

  echo "Creating Base VirtualEnv (${virtualenv_name})"

  mkvirtualenv.bat --python=python3.7.1.exe ${virtualenv_name}
  source $(determine_activate_script_path ${virtualenv_path})
  time pip install .[$(determine_pip_extras)]
  # Remove the specific version of Puma, as the pipeline-specific version will be installed later
  pip uninstall -y puma
}

function determine_pipeline_virtualenv_name {
  # Detemines a VirtualEnv's name based on the job type and current pipeline ID

  # Print out the base virtualenvs full id
  echo "puma-ci-$(determine_virtualenv_type)-${CI_PIPELINE_ID:-"unknown"}"
}

function ensure_pipeline_virtualenv_exists {
  # Ensures a pipeline VirtualEnv exists
  # This VirtualEnv is the same as the base VirtualEnv, except is also has this commit's version of PUMA installed

  pipeline_virtualenv_name=$(determine_pipeline_virtualenv_name)
  pipeline_virtualenv_path="${WORKON_HOME}/${pipeline_virtualenv_name}"

  # Detect outdated virtualenvs - pipeline VirtualEnvs that are over 30 days old
  cleanup_outdated_directories "${WORKON_HOME}" ".*/puma-ci-\(dev\|gpu\)-\([0-9]*\)"
  # Detect outdated tox environments that are over 30 days old
  cleanup_outdated_directories "${TOX_WORK_DIR_ROOT}" ".*/\([0-9]*\)"

  if [ -d "${pipeline_virtualenv_path}" ]; then
    echo "Pipeline VirtualEnv (${pipeline_virtualenv_name}) already exists, ending early"
    return
  fi

  ensure_base_virtualenv_exists
  base_virtualenv_name=$(determine_base_virtualenv_name)
  base_virtualenv_path="${WORKON_HOME}/${base_virtualenv_name}"

  echo "Creating Pipeline VirtualEnv (${pipeline_virtualenv_name})"

  # Copy the base VirtualEnv (this should be faster than creating an entirely new one)
  # Allow symlinks on Windows
  export MSYS=winsymlinks:nativestrict
  cp -rs "${base_virtualenv_path}/." ${pipeline_virtualenv_path}
  # Replace the virtualenv name in activate script
  activate_script_path=$(determine_activate_script_path ${pipeline_virtualenv_path})
  activate_script_path_original="${activate_script_path}.original"
  mv "${activate_script_path}" "${activate_script_path_original}"
  sed "s/${base_virtualenv_name}/${pipeline_virtualenv_name}/g" "${activate_script_path_original}" > "${activate_script_path}"
  rm "${activate_script_path_original}"

  source "${activate_script_path}"

  pip list

  # Ensure that PUMA doesn't exist in this VirtualEnv
  time pip uninstall -y puma

  pip list

  time pip install .[$(determine_pip_extras)]

  pip list
}

function cleanup_outdated_directories () {
  # Detect outdated directories (those that are over 7 days old)
  root_dir="$1"
  name_regex="$2"
  find_outdated_directories_command=(find "${root_dir}" -type d -mtime +7 -maxdepth 1 -regex "${name_regex}")
  echo "Searching for outdated directories in ${root_dir}"
  outdated_directories_count=$("${find_outdated_directories_command[@]}" | wc -l)
  echo "Found ${outdated_directories_count}"
  "${find_outdated_directories_command[@]}"
  
  if [ "${outdated_directories_count}" -gt "0" ]; then
    echo "Deleting outdated directories"
    # Delete those outdated directories
    delete_outdated_directories_command=("${find_outdated_directories_command[@]}")
    delete_outdated_directories_command+=(-exec rm -rf {} \;)
    # Print deletion command
    echo "${delete_outdated_directories_command[@]}"
    # Call deletion command
    "${delete_outdated_directories_command[@]}"
  fi
}