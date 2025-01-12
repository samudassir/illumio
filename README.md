# Make sure to have python environment setup
python3 -m virtualenv venv

# Activate virtualenv
source ./venv/bin/activate

# install dependencies
python3 -m pip install -r requirements.txt

# Run process-data.py
python3 process-data.py

# Deactivate virtualenv
deactivate