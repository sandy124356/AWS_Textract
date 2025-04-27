use either 
python -m piptools compile requirements.in
or
python -m pip-compile requirements.in

to generate requirements.txt (in detail)

Then you can install requirements from the newly generated requirements.txt

make sure you have configured .aws/config file with the necessary secret keys and access keys.
